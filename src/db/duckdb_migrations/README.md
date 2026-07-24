
###File Nameing Convention
 - NNNN_description_in_snake_case.sql 
 - naming pattern, 4-digit zero-padded, global sequence

Currently we are implmenting a Forward-only migration pattern to build out (no down-migrations as of yet) becasue we are looking to grow the database to fit the finalized needs of the data. If errors occure we can roll back to previous migrations
 
Embedded at compile time via include_str!, not read from disk at runtime — and the reasoning (single-binary distribution, consistent with your FEED_REGISTRY self-registering style)

The scope boundary: migrations/ covers fixed/global schema only (like _migrations itself); per-feed tables ({provider}__{symbol}__{feed_type}__{mode}) are provisioned dynamically in Rust, not tracked as migrations