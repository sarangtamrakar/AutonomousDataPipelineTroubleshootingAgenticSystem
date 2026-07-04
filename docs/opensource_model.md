# Open Source Model Recommendations

The premier open-source models optimized for a 16 GB M4 are categorized below by operational strengths.

## 1. The Reasoning / Agentic Powerhouse

**DeepSeek-R1-Distill-Qwen-8B**

- Best for: Complex logic, debugging deep agent code loops, and multi-step math/reasoning tasks.
- Performance: Around 24–28 tokens/sec on your chip.
- Ollama command:

```bash
ollama pull deepseek-r1:8b
```

**gpt-oss:20b (Highly Quantized Q2/Q3)**

- Best for: Pushing the absolute limits of step-by-step thinking models inside an agentic graph on a 16 GB machine without crashing.
- Ollama command:

```bash
ollama pull gpt-oss:20b
```

## 2. The Absolute Best for Coding

**Qwen2.5-Coder-7B** (or Qwen3-Coder family variations)

- Best for: Autocomplete, repository parsing, and writing clean Python/SQL code.
- Performance: Extremely fast on the M4, pushing 32–35+ tokens/sec.
- Ollama command:

```bash
ollama pull qwen2.5-coder:7b
```

## 3. The Best Everyday General Assistants

**Llama 3.1 8B / Llama 3.2 3B**

- Best for: Creative writing, general chat, basic JSON formatting, and day-to-day productivity.
- The 3B version runs completely in cache memory and responds almost instantly.
- Ollama command:

```bash
ollama pull llama3.1:8b
```

**Gemma 4 (e4b or e2b Variants)**

- Best for: Fast tool-calling execution and lighter agent tasks.
- Google's custom architectural builds for the edge run natively fast on the M4 neural engine vectors.
- Ollama command:

```bash
ollama pull gemma4:e4b
```
