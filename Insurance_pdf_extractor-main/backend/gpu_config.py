"""
GPU Acceleration & Hardware Configuration Manager

This module provides centralized hardware detection, dynamic concurrency scaling,
and automated GPU memory management for Rostaing-OCR, LLM parallel processing,
and PDF extraction pipelines across different server environments.

It ensures seamless fallback to CPU when CUDA/GPU is unavailable, preventing
Out-Of-Memory (OOM) errors while maximizing execution speed.
"""

import os
import sys
import subprocess
import logging
from typing import Dict, Any, Tuple

# Configure logging
logger = logging.getLogger("GPUConfigManager")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [GPU Manager] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

class GPUConfigManager:
    """
    Manages GPU acceleration, dynamic worker allocation, and environment setup
    across multiple server deployments.
    """
    
    def __init__(self):
        self.gpu_available = False
        self.device_name = "CPU (Default Fallback)"
        self.vram_total_gb = 0.0
        self.cuda_version = "N/A"
        self.onnx_gpu_available = False
        
        self._detect_hardware()

    def _detect_hardware(self):
        """Dynamically inspect active PyTorch and ONNX Runtime hardware capabilities."""
        # 1. Check PyTorch CUDA capabilities
        try:
            import torch
            if torch.cuda.is_available():
                self.gpu_available = True
                self.device_name = torch.cuda.get_device_name(0)
                # Calculate VRAM in GB
                if hasattr(torch.cuda, 'get_device_properties'):
                    props = torch.cuda.get_device_properties(0)
                    self.vram_total_gb = props.total_memory / (1024**3)
                
                # Get CUDA version
                self.cuda_version = torch.version.cuda or "N/A"
                logger.info(f"✅ NVIDIA GPU Detected: {self.device_name} ({self.vram_total_gb:.1f} GB VRAM, CUDA {self.cuda_version})")
            else:
                logger.warning("⚠️ PyTorch reports CUDA is unavailable. Checking if system has an NVIDIA GPU...")
                self._check_system_nvidia_gpu()
        except ImportError:
            logger.error("❌ PyTorch is not installed. Running in pure CPU fallback mode.")

        # 2. Check ONNX Runtime GPU capabilities (used by many OCR/layout models)
        try:
            import onnxruntime as ort
            providers = ort.get_available_providers()
            if 'CUDAExecutionProvider' in providers:
                self.onnx_gpu_available = True
                logger.info("✅ ONNX Runtime GPU (CUDAExecutionProvider) is available.")
            elif 'DmlExecutionProvider' in providers:
                self.onnx_gpu_available = True
                if not self.gpu_available:
                    self.gpu_available = True
                    self.device_name = "Intel Iris Xe / DirectML GPU"
                    self.vram_total_gb = 15.9  # Windows Shared VRAM
                logger.info("✅ ONNX Runtime DirectML (DmlExecutionProvider) is available for Intel Iris Xe GPU.")
            else:
                logger.info(f"ℹ️ ONNX Runtime running on CPU. Available providers: {providers}")
        except ImportError:
            logger.info("ℹ️ ONNX Runtime not installed.")

    def _check_system_nvidia_gpu(self):
        """Check if the physical server has an NVIDIA GPU but PyTorch is CPU-only."""
        try:
            # Run nvidia-smi to verify physical hardware presence
            result = subprocess.run(["nvidia-smi"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode == 0:
                logger.warning("\n" + "="*80)
                logger.warning("🚨 PHYSICAL NVIDIA GPU DETECTED ON SERVER, BUT PYTORCH IS RUNNING ON CPU! 🚨")
                logger.warning("This server has an NVIDIA GPU installed, but your Python environment has the CPU-only PyTorch wheel.")
                logger.warning("To fix this server instantly and enable 10x faster Rostaing-OCR, run this command in your terminal:")
                logger.warning("pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121")
                logger.warning("="*80 + "\n")
        except (FileNotFoundError, subprocess.SubprocessError):
            logger.info("ℹ️ No physical NVIDIA GPU detected on system. Proceeding cleanly with CPU Fallback.")

    def get_optimal_concurrency(self) -> Dict[str, Any]:
        """
        Calculates optimal worker counts and batch sizes depending on active hardware.
        Prevents CUDA Out-Of-Memory (OOM) on GPU while maximizing CPU threads on fallback.
        """
        cpu_cores = os.cpu_count() or 4

        if self.gpu_available:
            # --- GPU ACCELERATED MODE ---
            # GPUs process vision/OCR extremely fast sequentially but have strict VRAM limits
            ocr_workers = 2 if self.vram_total_gb >= 12.0 else 1
            
            return {
                "mode": "GPU",
                "device": "cuda",
                "rostaing_ocr": {
                    "max_workers": ocr_workers,
                    "batch_size": 4 if self.vram_total_gb >= 16.0 else 2,
                    "pin_memory": True
                },
                "llm_parallel_processing": {
                    # LLM API calls (OpenAI/Cloud) are I/O bound and do not consume local GPU VRAM
                    "max_workers": min(32, cpu_cores * 4),
                    "batch_size": 10
                },
                "local_llm_inference": {
                    # If running local LLMs (vLLM/HuggingFace) in VRAM
                    "max_workers": 1,
                    "max_batch_tokens": 4096
                },
                "pdf_rendering": {
                    # PDF to Image rasterization is CPU bound
                    "max_workers": min(8, cpu_cores)
                }
            }
        else:
            # --- CPU FALLBACK MODE ---
            # Maximize multi-core CPU parallelism
            return {
                "mode": "CPU",
                "device": "cpu",
                "rostaing_ocr": {
                    "max_workers": min(8, max(1, cpu_cores - 1)),
                    "batch_size": 1,
                    "pin_memory": False
                },
                "llm_parallel_processing": {
                    "max_workers": min(16, cpu_cores * 2),
                    "batch_size": 5
                },
                "local_llm_inference": {
                    "max_workers": 1,
                    "max_batch_tokens": 1024
                },
                "pdf_rendering": {
                    "max_workers": min(8, cpu_cores)
                }
            }

    def optimize_gpu_memory(self):
        """Configures PyTorch CUDA memory allocator to prevent VRAM fragmentation."""
        if not self.gpu_available:
            return

        try:
            import torch
            # Clear cached VRAM
            torch.cuda.empty_cache()
            
            # Set memory allocator configuration if supported (PyTorch 2.0+)
            if not os.getenv("PYTORCH_CUDA_ALLOC_CONF"):
                os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
                logger.info("ℹ️ Configured PyTorch CUDA allocator (expandable_segments:True) to prevent fragmentation.")
        except Exception as e:
            logger.warning(f"⚠️ Failed to optimize GPU memory: {e}")

    def clear_gpu_cache(self):
        """Explicitly clear PyTorch CUDA cache after heavy PDF extractions."""
        if self.gpu_available:
            try:
                import torch
                torch.cuda.empty_cache()
                logger.debug("🧹 Cleared PyTorch CUDA VRAM cache.")
            except Exception:
                pass

    def execute_with_rostaing(self, pdf_path: str, extraction_func) -> Tuple[str, Any]:
        """
        Wrapper function to safely execute Rostaing-OCR extraction with automatic
        pre/post GPU memory optimization.
        
        Args:
            pdf_path: Path to the PDF document.
            extraction_func: A callable that runs the rostaing extraction logic.
            
        Returns:
            Tuple of (extracted_text, metadata/results)
        """
        logger.info(f"🚀 Starting Rostaing-OCR execution on: {self.device_name}")
        self.optimize_gpu_memory()
        
        try:
            # Execute the provided rostaing extraction function
            result = extraction_func(pdf_path)
            return result
        finally:
            # Ensure VRAM is flushed immediately after completion
            self.clear_gpu_cache()


# Create a global singleton instance for easy importing across the codebase
gpu_manager = GPUConfigManager()
gpu_concurrency_config = gpu_manager.get_optimal_concurrency()

if __name__ == "__main__":
    # Print diagnostic summary when executed directly
    print("\n" + "="*80)
    print(f"🖥️  GPU CONFIGURATION & HARDWARE DIAGNOSTIC")
    print("="*80)
    print(f"Device Mode          : {gpu_concurrency_config['mode']}")
    print(f"Device Name          : {gpu_manager.device_name}")
    print(f"Total VRAM           : {gpu_manager.vram_total_gb:.1f} GB")
    print(f"CUDA Version         : {gpu_manager.cuda_version}")
    print(f"ONNX GPU Accelerated : {gpu_manager.onnx_gpu_available}")
    print("-" * 80)
    print("🚀 OPTIMAL CONCURRENCY SETTINGS:")
    for section, settings in gpu_concurrency_config.items():
        if isinstance(settings, dict):
            print(f"  ▪ {section.replace('_', ' ').title()}:")
            for k, v in settings.items():
                print(f"      {k:<20}: {v}")
    print("="*80 + "\n")
