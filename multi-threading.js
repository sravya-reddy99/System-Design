const { Worker, isMainThread, parentPort, workerData } = require("node:worker_threads");

// ---------------------------------------------------------------
// The work: count primes up to a limit
// ---------------------------------------------------------------

// Each thread has its own copy of this variable. It counts the checks done,
// which is our measure of work. A small or even number is settled in a few
// checks. A big prime needs about sqrt(n) / 2 checks.
let checks = 0;

function isPrime(n) {
  if (n < 2) return false;
  checks++;
  if (n % 2 === 0) return n === 2;
  for (let d = 3; d * d <= n; d += 2) {
    checks++;
    if (n % d === 0) return false;
  }
  return true;
}

// Count primes in [start, stop)
function countRange(start, stop) {
  let found = 0;
  for (let n = start; n < stop; n++) {
    if (isPrime(n)) found++;
  }
  return found;
}

// ---------------------------------------------------------------
// What runs INSIDE each worker thread
// ---------------------------------------------------------------

function workerMain(job) {
  const began = performance.now();
  let found = 0;

  if (job.mode === "static") {
    // A fixed range, decided before we started
    found = countRange(job.start, job.stop);
  } else {
    // Take the next piece from a shared counter, over and over
    const counter = new Int32Array(job.shared);
    while (true) {
      // Atomics.add adds and gives back the OLD value in one unbreakable step,
      // so two workers can never get the same piece. No lock needed.
      const start = Atomics.add(counter, 0, job.chunk);
      if (start > job.limit) break;
      found += countRange(start, Math.min(start + job.chunk, job.limit + 1));
    }
  }

  parentPort.postMessage({
    id: job.id,
    found,
    work: checks,
    busyMs: performance.now() - began,
  });
}

// ---------------------------------------------------------------
// What runs in the MAIN thread
// ---------------------------------------------------------------

function runWorkers(jobs) {
  return new Promise((resolve, reject) => {
    const began = performance.now();
    const results = [];
    for (const job of jobs) {
      const worker = new Worker(__filename, { workerData: job });
      worker.once("error", reject);
      worker.once("message", (result) => {
        results.push(result);
        if (results.length === jobs.length) {
          results.sort((a, b) => a.id - b.id);
          resolve({ results, ms: performance.now() - began });
        }
      });
    }
  });
}

function runStatic(limit, workers) {
  const size = Math.ceil((limit - 1) / workers); // numbers 2 .. limit
  const jobs = [];
  for (let i = 0; i < workers; i++) {
    const start = 2 + i * size;
    const stop = Math.min(start + size, limit + 1);
    jobs.push({ mode: "static", id: i, start, stop });
  }
  return runWorkers(jobs);
}

function runDynamic(limit, workers, chunk) {
  // Memory that every thread can see. Starts at 2, the first number to test.
  const shared = new SharedArrayBuffer(4);
  Atomics.store(new Int32Array(shared), 0, 2);
  const jobs = [];
  for (let i = 0; i < workers; i++) {
    jobs.push({ mode: "dynamic", id: i, limit, chunk, shared });
  }
  return runWorkers(jobs);
}

// How unevenly did the workers finish? Everyone starts together, so the
// worker that was busy longest decides when the whole job is done, and the
// others sit idle after they finish.
function stats(results) {
  const slowest = Math.max(...results.map((r) => r.busyMs));
  const average = results.reduce((sum, r) => sum + r.busyMs, 0) / results.length;
  return { slowest, average, ratio: slowest / average, idle: 100 * (1 - average / slowest) };
}

function report({ results, ms }, expected) {
  const s = stats(results);
  for (const r of results) {
    const bar = "#".repeat(Math.round((30 * r.busyMs) / s.slowest));
    console.log(
      `   worker ${r.id}: ${(r.work / 1e6).toFixed(1).padStart(5)}M checks  ` +
        `${(r.busyMs / 1000).toFixed(2).padStart(5)}s busy  ${bar}`
    );
  }
  const total = results.reduce((sum, r) => sum + r.found, 0);
  if (total !== expected) throw new Error(`wrong answer: ${total} != ${expected}`);
  console.log(
    `   total time ${(ms / 1000).toFixed(2)}s | slowest worker took ${s.ratio.toFixed(2)}x ` +
      `the average time | ${s.idle.toFixed(0)}% of the team idle while waiting for it`
  );
}

async function main() {
  const limit = Number(process.argv[2]) || 5_000_000;
  const workers = Number(process.argv[3]) || 4;
  console.log(`Counting primes up to ${limit.toLocaleString()} with ${workers} workers\n`);

  console.log("1. One thread, no sharing");
  const began = performance.now();
  checks = 0;
  const expected = countRange(2, limit + 1);
  const singleMs = performance.now() - began;
  console.log(
    `   ${expected.toLocaleString()} primes, ${(checks / 1e6).toFixed(1)}M checks, ` +
      `${(singleMs / 1000).toFixed(2)}s\n`
  );

  console.log("2. A fixed range for each worker");
  const fixed = await runStatic(limit, workers);
  report(fixed, expected);

  console.log("\n3. Hand out one number at a time, to whoever is free");
  const fair = await runDynamic(limit, workers, 1);
  report(fair, expected);

  console.log("\n4. Bonus: hand out bigger pieces instead of one number");
  let pieces = null;
  for (const chunk of [1, 100, 10_000]) {
    const run = await runDynamic(limit, workers, chunk);
    const s = stats(run.results);
    console.log(
      `   piece of ${chunk.toLocaleString().padStart(6)} numbers: ${(run.ms / 1000).toFixed(2)}s | ` +
        `slowest worker took ${s.ratio.toFixed(2)}x the average time`
    );
    pieces = run; // keep the last one (the biggest piece) for the summary
  }

  // Side by side summary of everything above
  const row = (name, ms, run) => {
    const s = run ? stats(run.results) : null;
    console.log(
      `   ${name.padEnd(34)} ${(ms / 1000).toFixed(2).padStart(6)}s   ` +
        `${(s ? s.ratio.toFixed(2) + "x" : "n/a").padStart(7)}   ` +
        `${(s ? s.idle.toFixed(0) + "%" : "n/a").padStart(5)}`
    );
  };
  console.log(`\nSUMMARY: primes up to ${limit.toLocaleString()}, ${workers} workers`);
  console.log(`   ${"approach".padEnd(34)} ${"time".padStart(7)}   ${"slowest".padStart(7)}   ${"idle".padStart(5)}`);
  row("1. One thread", singleMs, null);
  row("2. Fixed range per worker", fixed.ms, fixed);
  row("3. Fair, one number at a time", fair.ms, fair);
  row("3b. Fair, pieces of 10,000", pieces.ms, pieces);
  console.log("   (slowest = slowest worker compared with the average worker)");
}

if (isMainThread) {
  main();
} else {
  workerMain(workerData);
}