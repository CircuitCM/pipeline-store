A Pipeline library that does a few things differently to achieve flexibility and abstraction.

Both chains and graphs can be defined in an independent manner from the function details. A `Pipeline` class defines the dependencies for the `Steps` of a chain, and pipeline objects may be swapped in/out.

See the [demo](demo.md) for how to define a concurrent LLM procedure:
```mermaid
graph TD
  A1["Load Corpus"] --> A2["s_style3 + retrieval1"]
  A2 --> B3["s_style1 + query1"]
  A2 --> B4["s_style2 + query1"]
  A2 --> B5["s_style3 + query1"]
  B3 --> C6["Join Text"]
  B4 --> C6
  B5 --> C6
  A2 --> C6
  C6 --> D7["s_style4 + query2 -> result"]
```
