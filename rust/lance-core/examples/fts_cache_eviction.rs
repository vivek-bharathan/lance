// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! ENT-1216 — FTS index-cache eviction stress harness.
//!
//! Reproduces, at the cache layer, TinyLFU favouring stale out-dated FTS index
//! entries over the index actually being queried, and isolates the policy by
//! re-running the identical workload under LRU and under version-invalidation.
//!
//! Run the report:   `cargo run -p lance-core --example fts_cache_eviction`
//! Run helper tests:  `cargo test -p lance-core --example fts_cache_eviction`

// This is a report example: stdout IS its output, so the workspace-wide
// `print_stdout = "deny"` (use-log-not-println) rule does not apply here.
#![allow(dead_code, unused_imports, clippy::print_stdout)]

use std::sync::Arc;

use lance_core::cache::{CacheBackend, CacheEntry, InternalCacheKey, MokaCacheBackend};
use moka::future::Cache as MokaCache;
use moka::policy::EvictionPolicy;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};

/// Static type tag for all entries this harness inserts.
const TYPE_TAG: &str = "fts-stress";

/// Version-scoped key prefix. The trailing slash makes `invalidate_prefix`
/// match a single version exactly (so `v1/` never matches `v12/`).
fn version_prefix(index: usize, version: usize) -> String {
    format!("idx{index}/v{version}/")
}

fn key(index: usize, version: usize, sub: &str) -> InternalCacheKey {
    InternalCacheKey::new(
        Arc::from(version_prefix(index, version)),
        Arc::from(sub),
        TYPE_TAG,
    )
}

/// Partition entry — these are what `InvertedIndex::load` reloads O(num_partitions).
fn part_key(index: usize, version: usize, part: usize) -> InternalCacheKey {
    key(index, version, &format!("part-{part}"))
}

/// Token posting-list entry — the bulk of the entry count.
fn postings_key(index: usize, version: usize, token: usize) -> InternalCacheKey {
    key(index, version, &format!("postings-{token}"))
}

/// Build a Zipf distribution over `tokens` items with exponent `exponent`.
/// Sampled values fall in `[1, tokens]`.
fn make_zipf(tokens: usize, exponent: f64) -> Zipf<f64> {
    Zipf::new(tokens as f64, exponent).expect("valid zipf params")
}

/// Per-query token-access distribution.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dist {
    /// Skewed: a few head tokens dominate, long cold tail (realistic text search).
    Zipf,
    /// Flat: every token equally likely — ablation that removes the head/tail skew.
    Uniform,
}

impl Dist {
    fn label(self) -> &'static str {
        match self {
            Self::Zipf => "zipf",
            Self::Uniform => "uniform",
        }
    }
    fn all() -> [Self; 2] {
        [Self::Zipf, Self::Uniform]
    }
    fn parse(s: &str) -> Option<Self> {
        match s {
            "zipf" => Some(Self::Zipf),
            "uniform" => Some(Self::Uniform),
            _ => None,
        }
    }
}

/// A constructed per-query token sampler for one `Dist` mode.
#[derive(Clone, Copy)]
enum TokenSampler {
    Zipf(Zipf<f64>),
    Uniform,
}

impl TokenSampler {
    fn new(dist: Dist, tokens: usize, exponent: f64) -> Self {
        match dist {
            Dist::Zipf => Self::Zipf(make_zipf(tokens, exponent)),
            Dist::Uniform => Self::Uniform,
        }
    }

    /// Sample a 0-based token id in `0..tokens`.
    fn sample(self, rng: &mut SmallRng, tokens: usize) -> usize {
        match self {
            // Zipf yields a float in [1, tokens]; map to 0-based and clamp.
            Self::Zipf(z) => (z.sample(rng) as usize).saturating_sub(1).min(tokens - 1),
            Self::Uniform => rng.random_range(0..tokens),
        }
    }
}

/// Uniform async interface over the cache variants under test.
enum ArmCache {
    /// The real production backend (default TinyLFU). Used for Baseline and Evict-old.
    Backend(MokaCacheBackend),
    /// Hand-built moka cache mirroring `MokaCacheBackend::with_capacity`
    /// (see `src/cache/moka.rs:41-45`) but with `EvictionPolicy::lru()`.
    Lru(MokaCache<InternalCacheKey, (CacheEntry, usize)>),
}

impl ArmCache {
    fn moka(capacity: usize) -> Self {
        Self::Backend(MokaCacheBackend::with_capacity(capacity))
    }

    fn lru(capacity: usize) -> Self {
        let cache = MokaCache::builder()
            .max_capacity(capacity as u64)
            .weigher(|_k, v: &(CacheEntry, usize)| v.1.try_into().unwrap_or(u32::MAX))
            .support_invalidation_closures()
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self::Lru(cache)
    }

    /// Returns true on cache hit.
    async fn get(&self, key: &InternalCacheKey) -> bool {
        match self {
            Self::Backend(b) => b.get(key, None).await.is_some(),
            Self::Lru(c) => c.get(key).await.is_some(),
        }
    }

    async fn insert(&self, key: &InternalCacheKey, size_bytes: usize) {
        let entry: CacheEntry = Arc::new(()); // ~0 real bytes; size is the accounting knob
        match self {
            Self::Backend(b) => b.insert(key, entry, size_bytes, None).await,
            Self::Lru(c) => c.insert(key.clone(), (entry, size_bytes)).await,
        }
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        match self {
            Self::Backend(b) => b.invalidate_prefix(prefix).await,
            Self::Lru(c) => {
                // invalidate_entries_if schedules removal synchronously (no await);
                // run_pending() flushes it before the next measurement.
                let prefix = prefix.to_owned();
                c.invalidate_entries_if(move |k, _v| k.starts_with(&prefix))
                    .expect("invalidation closures enabled");
            }
        }
    }

    /// Flush moka's deferred eviction/insertion bookkeeping before measuring.
    async fn run_pending(&self) {
        match self {
            Self::Backend(b) => {
                // The backend exposes no direct flush, so we drive it via num_entries,
                // which runs pending tasks internally; the returned count is discarded.
                // (Note: a later num_entries() call will flush again — cheap, harmless.)
                b.num_entries().await;
            }
            Self::Lru(c) => c.run_pending_tasks().await,
        }
    }

    async fn num_entries(&self) -> usize {
        match self {
            Self::Backend(b) => b.num_entries().await,
            Self::Lru(c) => {
                c.run_pending_tasks().await;
                c.entry_count() as usize
            }
        }
    }
}

/// Snapshot of cumulative counters at a round boundary, for windowed rates.
#[derive(Clone, Copy)]
struct Checkpoint {
    round: usize,
    ws_gets: u64,
    ws_hits: u64,
}

/// Working-set hit rate over the window between two checkpoints.
fn windowed_hit_rate(start: &Checkpoint, end: &Checkpoint) -> f64 {
    let gets = end.ws_gets.saturating_sub(start.ws_gets);
    let hits = end.ws_hits.saturating_sub(start.ws_hits);
    if gets == 0 { 0.0 } else { hits as f64 / gets as f64 }
}

#[derive(Default)]
struct ArmStats {
    /// Gets on current-version working-set entries.
    ws_gets: u64,
    ws_hits: u64,
    /// Re-inserts forced by working-set misses (proxy for ~4s reloads).
    hot_reloads: u64,
    /// Mode B: probes of just-inserted current-version entries, and how many survived.
    nv_probes: u64,
    nv_survived: u64,
    /// Final cache entry count.
    entries: usize,
    /// Per-round checkpoints for the evolution table.
    checkpoints: Vec<Checkpoint>,
}

impl ArmStats {
    fn ws_hit_rate(&self) -> f64 {
        if self.ws_gets == 0 { 0.0 } else { self.ws_hits as f64 / self.ws_gets as f64 }
    }
    fn nv_survival_rate(&self) -> f64 {
        if self.nv_probes == 0 { 0.0 } else { self.nv_survived as f64 / self.nv_probes as f64 }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    /// Production today: TinyLFU, no version invalidation.
    Baseline,
    /// Same workload, LRU eviction — isolates the policy.
    Lru,
    /// TinyLFU + invalidate the previous version's entries on each bump — the
    /// *kind* of fix ENT-1216 needs.
    EvictOld,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "Baseline (TinyLFU)",
            Self::Lru => "LRU",
            Self::EvictOld => "Evict-old versions",
        }
    }
    fn all() -> [Self; 3] {
        [Self::Baseline, Self::Lru, Self::EvictOld]
    }
}

#[derive(Clone, Copy)]
struct Params {
    num_hot_indexes: usize,
    partitions: usize,
    tokens: usize,
    queries_per_round: usize,
    terms_per_query: usize,
    zipf_exponent: f64,
    /// Per-query token access distribution (zipf | uniform).
    dist: Dist,
    versions: usize,
    /// Logical-vs-real size overcount factor observed at reevo (~80x).
    size_inflation: usize,
    base_entry_bytes: usize,
    /// Accounted-byte eviction ceiling. Sized so a handful of versions saturate it.
    capacity: usize,
    seed: u64,
}

impl Default for Params {
    fn default() -> Self {
        // Anchored to reevo: 2 hot tables (note, chat_message_turn), small partition
        // count, thousands of tokens, constant version churn from single-row merge_inserts.
        Self {
            num_hot_indexes: 2,
            partitions: 4,
            tokens: 5_000,
            queries_per_round: 50,
            terms_per_query: 3,
            zipf_exponent: 1.1,
            dist: Dist::Zipf,
            versions: 400,
            size_inflation: 80,
            base_entry_bytes: 4_096,
            // ~5 versions' worth of inflated bytes: forces saturation + churn,
            // while real payload (Arc<()>) is ~0, mirroring "phantom-full".
            capacity: 8_000_000_000,
            seed: 42,
        }
    }
}

impl Params {
    fn entry_size_bytes(&self) -> usize {
        self.base_entry_bytes * self.size_inflation
    }

    fn with_overrides(mut self, overrides: &[(&str, &str)]) -> Self {
        for (k, v) in overrides {
            match *k {
                "num_hot_indexes" => self.num_hot_indexes = v.parse().unwrap_or(self.num_hot_indexes),
                "partitions" => self.partitions = v.parse().unwrap_or(self.partitions),
                "tokens" => self.tokens = v.parse().unwrap_or(self.tokens),
                "queries_per_round" => self.queries_per_round = v.parse().unwrap_or(self.queries_per_round),
                "terms_per_query" => self.terms_per_query = v.parse().unwrap_or(self.terms_per_query),
                "zipf_exponent" => self.zipf_exponent = v.parse().unwrap_or(self.zipf_exponent),
                "dist" => self.dist = Dist::parse(v).unwrap_or(self.dist),
                "versions" => self.versions = v.parse().unwrap_or(self.versions),
                "size_inflation" => self.size_inflation = v.parse().unwrap_or(self.size_inflation),
                "base_entry_bytes" => self.base_entry_bytes = v.parse().unwrap_or(self.base_entry_bytes),
                "capacity" => self.capacity = v.parse().unwrap_or(self.capacity),
                "seed" => self.seed = v.parse().unwrap_or(self.seed),
                _ => {} // ignore unknown keys
            }
        }
        self
    }
}

fn build_cache(arm: Arm, params: &Params) -> ArmCache {
    match arm {
        Arm::Lru => ArmCache::lru(params.capacity),
        Arm::Baseline | Arm::EvictOld => ArmCache::moka(params.capacity),
    }
}

/// How often to snapshot a checkpoint, as a fraction of total rounds.
const CHECKPOINTS: usize = 10;

async fn run_arm(arm: Arm, params: &Params) -> ArmStats {
    let cache = build_cache(arm, params);
    let mut rng = SmallRng::seed_from_u64(params.seed);
    let sampler = TokenSampler::new(params.dist, params.tokens, params.zipf_exponent);
    let size = params.entry_size_bytes();
    let mut stats = ArmStats::default();

    // Current version number per hot index (0 = none yet).
    let mut versions = vec![0usize; params.num_hot_indexes];
    let checkpoint_every = (params.versions / CHECKPOINTS).max(1);

    for round in 0..params.versions {
        let idx = round % params.num_hot_indexes;
        let old_v = versions[idx];
        let new_v = old_v + 1;
        versions[idx] = new_v;

        // Evict-old: drop the previous version's entries before inserting the new ones.
        if arm == Arm::EvictOld && old_v > 0 {
            cache.invalidate_prefix(&version_prefix(idx, old_v)).await;
        }

        // Insert the new version: partition entries + token posting entries.
        for part in 0..params.partitions {
            cache.insert(&part_key(idx, new_v, part), size).await;
        }
        for tok in 0..params.tokens {
            cache.insert(&postings_key(idx, new_v, tok), size).await;
        }
        cache.run_pending().await;

        // Mode B — did the just-inserted current-version partition entries survive
        // the churn of inserting the rest (admission rejection / immediate eviction)?
        for part in 0..params.partitions {
            stats.nv_probes += 1;
            if cache.get(&part_key(idx, new_v, part)).await {
                stats.nv_survived += 1;
            }
        }

        // Queries: each touches all partitions + `terms_per_query` Zipfian tokens of a
        // randomly chosen hot index's current version. Reload (re-insert) on miss.
        for _ in 0..params.queries_per_round {
            let qidx = rng.random_range(0..params.num_hot_indexes);
            let v = versions[qidx];
            if v == 0 {
                continue; // index not yet created this run
            }
            for part in 0..params.partitions {
                stats.ws_gets += 1;
                if cache.get(&part_key(qidx, v, part)).await {
                    stats.ws_hits += 1;
                } else {
                    stats.hot_reloads += 1;
                    cache.insert(&part_key(qidx, v, part), size).await;
                }
            }
            for _ in 0..params.terms_per_query {
                let tok = sampler.sample(&mut rng, params.tokens);
                stats.ws_gets += 1;
                if cache.get(&postings_key(qidx, v, tok)).await {
                    stats.ws_hits += 1;
                } else {
                    stats.hot_reloads += 1;
                    cache.insert(&postings_key(qidx, v, tok), size).await;
                }
            }
        }

        if round % checkpoint_every == 0 || round + 1 == params.versions {
            // The two conditions can both fire on the final round; dedup so the
            // report's windows(2) pass doesn't see a zero-length trailing window.
            let already_pushed = stats.checkpoints.last().is_some_and(|c| c.round == round);
            if !already_pushed {
                stats.checkpoints.push(Checkpoint {
                    round,
                    ws_gets: stats.ws_gets,
                    ws_hits: stats.ws_hits,
                });
            }
        }
    }

    stats.entries = cache.num_entries().await;
    stats
}

#[tokio::main]
async fn main() {
    // Parse `key=value` args (e.g. `cargo run ... -- versions=200 seed=7`).
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let pairs: Vec<(&str, &str)> = raw
        .iter()
        .filter_map(|a| a.split_once('='))
        .collect();
    let base = Params::default().with_overrides(&pairs);

    // If the user pinned a distribution (`dist=...`), run only that; otherwise
    // run both Zipfian and uniform so the result can be compared across them.
    let dists: Vec<Dist> = if pairs.iter().any(|(k, _)| *k == "dist") {
        vec![base.dist]
    } else {
        Dist::all().to_vec()
    };

    println!("ENT-1216 — FTS index-cache eviction stress harness");
    println!(
        "params: hot_indexes={} partitions={} tokens={} versions={} \
         queries/round={} terms/query={} zipf_s={} inflation={}x capacity={} seed={}",
        base.num_hot_indexes, base.partitions, base.tokens, base.versions,
        base.queries_per_round, base.terms_per_query, base.zipf_exponent,
        base.size_inflation, base.capacity, base.seed,
    );
    let live_entries = base.num_hot_indexes * (base.partitions + base.tokens);
    println!("live working-set entries (1 version/index): {live_entries}");

    for dist in dists {
        let params = Params { dist, ..base };
        println!("\n=== token distribution: {} ===", dist.label());
        println!(
            "{:<22} {:>12} {:>14} {:>12} {:>10} {:>14}",
            "arm", "ws_hit_rate", "newver_surv", "hot_reloads", "entries", "deadweight%",
        );
        println!("{}", "-".repeat(88));

        let mut summaries = Vec::new();
        for arm in Arm::all() {
            let stats = run_arm(arm, &params).await;
            let deadweight = if stats.entries > live_entries {
                100.0 * (stats.entries - live_entries) as f64 / stats.entries as f64
            } else {
                0.0
            };
            println!(
                "{:<22} {:>11.1}% {:>13.1}% {:>12} {:>10} {:>13.1}%",
                arm.label(),
                100.0 * stats.ws_hit_rate(),
                100.0 * stats.nv_survival_rate(),
                stats.hot_reloads,
                stats.entries,
                deadweight,
            );
            summaries.push((arm, stats));
        }

        // Evolution: windowed working-set hit rate per checkpoint.
        println!("\nWindowed working-set hit rate by round:");
        for (arm, stats) in &summaries {
            print!("{:<22}", arm.label());
            for w in stats.checkpoints.windows(2) {
                print!(" {:>5.0}%", 100.0 * windowed_hit_rate(&w[0], &w[1]));
            }
            println!();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_prefix_includes_trailing_slash_and_version() {
        assert_eq!(version_prefix(0, 3), "idx0/v3/");
        assert_eq!(version_prefix(1, 12), "idx1/v12/");
    }

    #[test]
    fn entry_keys_share_their_version_prefix() {
        let p = version_prefix(0, 3);
        assert!(part_key(0, 3, 2).starts_with(&p));
        assert!(postings_key(0, 3, 99).starts_with(&p));
    }

    #[test]
    fn prefix_match_is_version_exact() {
        // A key from v12 must NOT match the v1 prefix (trailing slash disambiguates).
        let v1 = version_prefix(0, 1);
        assert!(!postings_key(0, 12, 5).starts_with(&v1));
        // ...and a different index must not match.
        assert!(!part_key(1, 1, 0).starts_with(&v1));
    }

    #[test]
    fn zipf_is_deterministic_for_a_seed() {
        let s = TokenSampler::new(Dist::Zipf, 1000, 1.1);
        let mut a = SmallRng::seed_from_u64(7);
        let mut b = SmallRng::seed_from_u64(7);
        let sa: Vec<usize> = (0..20).map(|_| s.sample(&mut a, 1000)).collect();
        let sb: Vec<usize> = (0..20).map(|_| s.sample(&mut b, 1000)).collect();
        assert_eq!(sa, sb);
    }

    #[test]
    fn zipf_stays_in_bounds() {
        let tokens = 500;
        let s = TokenSampler::new(Dist::Zipf, tokens, 1.1);
        let mut rng = SmallRng::seed_from_u64(1);
        for _ in 0..10_000 {
            let t = s.sample(&mut rng, tokens);
            assert!(t < tokens, "token {t} out of bounds {tokens}");
        }
    }

    #[test]
    fn zipf_favours_the_head() {
        let tokens = 1000;
        let s = TokenSampler::new(Dist::Zipf, tokens, 1.1);
        let mut rng = SmallRng::seed_from_u64(99);
        let (mut head, mut tail) = (0usize, 0usize);
        for _ in 0..50_000 {
            let t = s.sample(&mut rng, tokens);
            if t < tokens / 10 { head += 1; }
            if t >= tokens - tokens / 10 { tail += 1; }
        }
        // Head decile should be sampled far more than the tail decile.
        assert!(head > tail * 5, "head={head} tail={tail}");
    }

    #[test]
    fn uniform_is_in_bounds_and_deterministic() {
        let tokens = 500;
        let s = TokenSampler::new(Dist::Uniform, tokens, 0.0);
        let mut a = SmallRng::seed_from_u64(3);
        let mut b = SmallRng::seed_from_u64(3);
        for _ in 0..10_000 {
            let ta = s.sample(&mut a, tokens);
            let tb = s.sample(&mut b, tokens);
            assert!(ta < tokens, "token {ta} out of bounds {tokens}");
            assert_eq!(ta, tb, "same seed must reproduce");
        }
    }

    #[test]
    fn uniform_is_roughly_flat() {
        // Counterpart to zipf_favours_the_head: neither decile should dominate.
        let tokens = 1000;
        let s = TokenSampler::new(Dist::Uniform, tokens, 0.0);
        let mut rng = SmallRng::seed_from_u64(99);
        let (mut head, mut tail) = (0usize, 0usize);
        for _ in 0..50_000 {
            let t = s.sample(&mut rng, tokens);
            if t < tokens / 10 { head += 1; }
            if t >= tokens - tokens / 10 { tail += 1; }
        }
        // Within 2x of each other (Zipf's head/tail ratio is >>5x).
        assert!(head < tail * 2 && tail < head * 2, "head={head} tail={tail}");
    }

    #[tokio::test]
    async fn lru_evicts_when_over_capacity() {
        // Capacity holds ~10 entries of size 100; insert 200 → must evict.
        let cache = ArmCache::lru(1_000);
        for i in 0..200 {
            cache.insert(&part_key(0, 1, i), 100).await;
        }
        let n = cache.num_entries().await;
        assert!(n < 200, "expected eviction, got {n} entries");
        assert!(n > 0, "expected some entries retained, got {n}");
    }

    #[tokio::test]
    async fn lru_invalidate_prefix_removes_only_that_version() {
        let cache = ArmCache::lru(10_000_000);
        cache.insert(&part_key(0, 1, 0), 100).await;
        cache.insert(&part_key(0, 2, 0), 100).await;
        cache.invalidate_prefix(&version_prefix(0, 1)).await;
        cache.run_pending().await;
        assert!(!cache.get(&part_key(0, 1, 0)).await, "v1 should be gone");
        assert!(cache.get(&part_key(0, 2, 0)).await, "v2 should remain");
    }

    #[tokio::test]
    async fn moka_backend_arm_hits_then_invalidates() {
        let cache = ArmCache::moka(10_000_000);
        cache.insert(&part_key(0, 1, 0), 100).await;
        assert!(cache.get(&part_key(0, 1, 0)).await, "freshly inserted should hit");
        cache.invalidate_prefix(&version_prefix(0, 1)).await;
        cache.run_pending().await;
        assert!(!cache.get(&part_key(0, 1, 0)).await, "should miss after invalidate");
    }

    #[test]
    fn stats_compute_rates() {
        let mut s = ArmStats::default();
        s.ws_gets = 100;
        s.ws_hits = 75;
        s.nv_probes = 40;
        s.nv_survived = 10;
        assert!((s.ws_hit_rate() - 0.75).abs() < 1e-9);
        assert!((s.nv_survival_rate() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn stats_rates_are_zero_when_empty() {
        let s = ArmStats::default();
        assert_eq!(s.ws_hit_rate(), 0.0);
        assert_eq!(s.nv_survival_rate(), 0.0);
    }

    #[test]
    fn windowed_hit_rate_uses_deltas() {
        // Two checkpoints: between them, 50 gets / 40 hits → 0.8 windowed.
        let a = Checkpoint { round: 10, ws_gets: 100, ws_hits: 60 };
        let b = Checkpoint { round: 20, ws_gets: 150, ws_hits: 100 };
        assert!((windowed_hit_rate(&a, &b) - 0.8).abs() < 1e-9);
    }

    #[test]
    fn params_default_is_reevo_anchored() {
        let p = Params::default();
        assert_eq!(p.num_hot_indexes, 2);
        assert_eq!(p.size_inflation, 80);
        assert!(p.versions >= 100);
    }

    #[test]
    fn params_overrides_apply_and_ignore_unknown() {
        let p = Params::default()
            .with_overrides(&[("versions", "5"), ("seed", "99"), ("bogus", "1")]);
        assert_eq!(p.versions, 5);
        assert_eq!(p.seed, 99);
        assert_eq!(p.num_hot_indexes, 2); // untouched
    }

    #[test]
    fn entry_size_applies_inflation() {
        let p = Params::default();
        assert_eq!(p.entry_size_bytes(), p.base_entry_bytes * p.size_inflation);
    }

    #[test]
    fn dist_default_and_override() {
        assert_eq!(Params::default().dist, Dist::Zipf);
        let p = Params::default().with_overrides(&[("dist", "uniform")]);
        assert_eq!(p.dist, Dist::Uniform);
        // Unknown distribution value leaves the default untouched.
        let p2 = Params::default().with_overrides(&[("dist", "bogus")]);
        assert_eq!(p2.dist, Dist::Zipf);
    }

    #[tokio::test]
    async fn run_arm_produces_sane_stats() {
        let p = Params::default().with_overrides(&[
            ("versions", "10"), ("tokens", "50"), ("queries_per_round", "10"),
        ]);
        let s = run_arm(Arm::Baseline, &p).await;
        assert!(s.ws_gets > 0);
        assert!(s.ws_hits <= s.ws_gets);
        assert!(s.entries > 0);
    }

    #[tokio::test]
    async fn run_arm_works_under_uniform_distribution() {
        let p = Params::default().with_overrides(&[
            ("versions", "10"), ("tokens", "50"), ("queries_per_round", "10"),
            ("dist", "uniform"),
        ]);
        let s = run_arm(Arm::Baseline, &p).await;
        assert_eq!(p.dist, Dist::Uniform);
        assert!(s.ws_gets > 0);
        assert!(s.ws_hits <= s.ws_gets);
    }

    #[tokio::test]
    async fn huge_capacity_means_no_thrash() {
        // With capacity far above the total, nothing evicts → near-perfect hit rate.
        let p = Params::default().with_overrides(&[
            ("versions", "20"), ("tokens", "50"), ("queries_per_round", "20"),
            ("capacity", "1000000000000"),
        ]);
        let s = run_arm(Arm::Baseline, &p).await;
        assert!(s.ws_hit_rate() > 0.95, "hit rate {} should be ~1.0", s.ws_hit_rate());
    }

    #[tokio::test]
    async fn tiny_capacity_causes_misses() {
        // Capacity below one version's footprint → working set cannot stay resident.
        let p = Params::default().with_overrides(&[
            ("versions", "20"), ("tokens", "200"), ("queries_per_round", "20"),
            ("capacity", "5000000"),
        ]);
        let s = run_arm(Arm::Baseline, &p).await;
        assert!(s.ws_hit_rate() < 0.95, "hit rate {} should show misses", s.ws_hit_rate());
    }
}
