# Provider Dispatch: Static vs. Dynamic

**Decision: dynamic dispatch, via `async-trait`.**

## The two options

**Static (enum)** — every provider known at compile time, `match` picks the code:
```rust
enum Provider {
    Kraken(KrakenConnector),
}
```
Compiler-enforced exhaustiveness, zero runtime cost — but adding a provider always means editing this project's enum + every match on it.

**Dynamic (`dyn Provider`)** — providers implement a shared trait, collected as trait objects:
```rust
trait Provider {
    fn name(&self) -> &str;
}

let providers: Vec<Box<dyn Provider>> = vec![Box::new(KrakenConnector)];
```
Adding a provider = new file implementing the trait, nothing else to touch. No compiler safety net for a forgotten wire-up.

## Why dynamic wins here

Providers get added opportunistically with no fixed roadmap (confirmed scope) — that's exactly the shape `dyn` is good at. Static dispatch would mean re-touching this project's core dispatch code every time.

## The complication, and why it's not a blocker

Trait methods can't return `impl Future<...>` and still be used as `dyn Trait` — the compiler needs every implementor's method to fit the same fixed-size vtable slot, but each `async fn` compiles to its own differently-sized, compiler-generated "resume state" struct. Boxing (`Box<dyn Future<...>>`) turns that into a uniform pointer-sized handle, which does fit.

`async-trait` does that rewrite for you:
```rust
#[async_trait]
trait Provider {
    async fn connect(&self) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl Provider for KrakenConnector {
    async fn connect(&self) -> Result<(), anyhow::Error> {
        // written exactly like normal async code
    }
}
```
Cost: one dependency, one attribute per trait/impl, a small heap allocation per call (irrelevant at this data rate), slightly less readable compiler errors when something's wrong. Not enough to outweigh the fit for an open-ended provider list.
