import os
import time
import torch
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer

# Import your custom high-speed engine
import unchecked_io

# --- CONFIGURATION ---
# Switching to GPT-2 for stability. It is small, fast, and has a standard tokenizer
# that won't crash older/newer Rust crate versions.
MODEL_ID = "gpt2"
TOKENIZER_FILE = "tokenizer.json"

# Global state container
model_state = {
    "model": None,
    "rust_engine": None,
    "py_tokenizer": None
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"\n--- 🚀 UNCHECKED SERVER STARTUP ---")

    # 1. Fetch Tokenizer (Python side)
    print(f"1. Fetching Tokenizer Config from {MODEL_ID}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_fast=True)
    tokenizer.save_pretrained(".")
    model_state["py_tokenizer"] = tokenizer

    # 2. Initialize UncheckedIO (Rust + CUDA)
    print(f"2. Initializing UncheckedIO (Fuel Injector)...")
    try:
        model_state["rust_engine"] = unchecked_io.TokenizerEngine(TOKENIZER_FILE)
        print("   ✅ Rust Engine Ready (Zero-Copy Pipeline Active)")
    except Exception as e:
        print(f"   ❌ Failed to load Rust Engine: {e}")
        raise e

    # 3. Load Model (PyTorch)
    print(f"3. Loading Model Weights...")
    try:
        model_state["model"] = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            device_map="cuda",
            torch_dtype=torch.float16
        )
        print(f"   ✅ Model Loaded on {model_state['model'].device}")
    except Exception as e:
        print(f"   ❌ Failed to load model: {e}")
        raise e

    print("--- SERVER READY ---\n")
    yield
    print("\n--- SERVER SHUTDOWN ---")
    # Resources are cleaned up here

app = FastAPI(title="UncheckedIO High-Speed Server", lifespan=lifespan)

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 50
    temperature: float = 0.7

@app.post("/generate")
async def generate_text(req: GenerateRequest):
    model = model_state["model"]
    engine = model_state["rust_engine"]
    tokenizer = model_state["py_tokenizer"]

    if not model or not engine:
        raise HTTPException(status_code=503, detail="Server not ready")

    try:
        # --- PHASE 1: UNCHECKED INGESTION (Rust) ---
        # 4.8x Faster than standard .to("cuda")
        input_capsule = engine.encode_batch([req.prompt])
        input_ids = torch.from_dlpack(input_capsule)

        # --- PHASE 2: INFERENCE (PyTorch) ---
        with torch.no_grad():
            output_ids = model.generate(
                input_ids,
                max_new_tokens=req.max_tokens,
                temperature=req.temperature,
                do_sample=True,
                pad_token_id=tokenizer.eos_token_id
            )

        # --- PHASE 3: DECODING ---
        generated_text = tokenizer.decode(output_ids[0], skip_special_tokens=True)

        return {
            "response": generated_text,
            "backend": "UncheckedIO + PyTorch",
            "status": "success"
        }

    except Exception as e:
        print(f"Error during generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)