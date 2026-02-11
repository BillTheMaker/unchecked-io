import time
import torch
import os
import json
import unchecked_io
from transformers import AutoTokenizer

# --- CONFIGURATION ---
MODEL_ID = "bert-base-uncased"
TOKENIZER_PATH = "tokenizer.json"
BATCH_SIZE = 4096      # Massive batch to saturate PCIe
NUM_BATCHES = 50       # Run enough to stabilize GPU clock
SEQ_LEN = 128          # Typical sentence length
WARMUP = 5

# Synthetic Data
SAMPLE_TEXT = "The quick brown fox jumps over the lazy dog. " * 5  # ~50 tokens
DATA_BATCH = [SAMPLE_TEXT for _ in range(BATCH_SIZE)]

print(f"--- GPU PIPELINE BENCHMARK (RTX 3060) ---")
print(f"Batch Size: {BATCH_SIZE} | Batches: {NUM_BATCHES}")
print(f"Total Strings processed: {BATCH_SIZE * NUM_BATCHES:,}")

# --- 1. SETUP RESOURCES ---

# A. Prepare Tokenizer File for Rust
if not os.path.exists(TOKENIZER_PATH):
    print(f"Downloading {MODEL_ID} tokenizer...")
    # forcing use_fast=True ensures we get the JSON file Rust needs
    hf_tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_fast=True)
    hf_tokenizer.save_pretrained(".")
    print("Saved tokenizer.json")
else:
    print("Found existing tokenizer.json")
    hf_tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_fast=True)

# B. Initialize Engines
print("Initializing Engines...")

# UncheckedIO (Rust + CUDA)
try:
    rust_engine = unchecked_io.TokenizerEngine(TOKENIZER_PATH)
    print("✅ UncheckedIO Engine Loaded")
except AttributeError:
    print("❌ Error: UncheckedIO 'TokenizerEngine' not found.")
    print("Did you compile with: maturin develop --features gpu")
    exit(1)

# --- 2. BENCHMARK LOOPS ---

def benchmark_standard():
    torch.cuda.synchronize()
    start = time.time()

    for _ in range(NUM_BATCHES):
        # Step 1: Tokenize (CPU Python/Rust mix)
        # return_tensors='pt' creates a CPU tensor
        encodings = hf_tokenizer(
            DATA_BATCH,
            padding=True,
            truncation=True,
            max_length=SEQ_LEN,
            return_tensors="pt"
        )

        # Step 2: Move to GPU (The Bottleneck)
        input_ids = encodings["input_ids"].to("cuda", non_blocking=True)

        # Force sync to measure actual completion
        torch.cuda.synchronize()

    return time.time() - start

def benchmark_unchecked():
    torch.cuda.synchronize()
    start = time.time()

    for _ in range(NUM_BATCHES):
        # Step 1 & 2: Tokenize + DMA to GPU (All in Rust)
        dlpack_capsule = rust_engine.encode_batch(DATA_BATCH)

        # Step 3: Zero-Copy Wrap (Python)
        # This is virtually instant
        input_ids = torch.from_dlpack(dlpack_capsule)

        # Force sync
        torch.cuda.synchronize()

    return time.time() - start

# --- 3. RUN RACES ---

print("\nWARMING UP GPU...")
# Run a few dummy passes to wake up the 3060
_ = hf_tokenizer(DATA_BATCH[:10], return_tensors="pt")["input_ids"].to("cuda")
_ = torch.from_dlpack(rust_engine.encode_batch(DATA_BATCH[:10]))

print("\n🚀 RUNNING STANDARD PIPELINE (HF -> CPU Tensor -> CUDA)...")
time_std = benchmark_standard()
fps_std = (BATCH_SIZE * NUM_BATCHES) / time_std
print(f"Time: {time_std:.4f}s | Throughput: {fps_std:,.0f} samples/sec")

print("\n🚀 RUNNING UNCHECKED PIPELINE (Rust -> Pinned -> CUDA)...")
time_unchecked = benchmark_unchecked()
fps_unchecked = (BATCH_SIZE * NUM_BATCHES) / time_unchecked
print(f"Time: {time_unchecked:.4f}s | Throughput: {fps_unchecked:,.0f} samples/sec")

# --- 4. RESULTS ---

speedup = fps_unchecked / fps_std
print(f"\n🏆 WINNER: {'UncheckedIO' if speedup > 1 else 'Standard'}")
print(f"SPEEDUP FACTOR: {speedup:.2f}x")
print(f"Standard Latency per Batch: {(time_std/NUM_BATCHES)*1000:.2f}ms")
print(f"Unchecked Latency per Batch: {(time_unchecked/NUM_BATCHES)*1000:.2f}ms")