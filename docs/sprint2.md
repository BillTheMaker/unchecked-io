Sprint 2: Streaming Parser

# 🏁 MVP Sprint 2: Streaming Parser Implementation

**Goal:** Refactor `src/parser.rs` to parse the Postgres binary stream _as it arrives_, rather than buffering the entire 130MB+ result set into a `Vec<u8>` first. This will significantly reduce memory overhead and improve processing latency.

### Phase 1: Refactor `handle_binary_copy` State

This phase involves changing the _structure_ of the function to support streaming.

- [ ] **Task 1: Initialize Builders First**

    - In `handle_binary_copy`, move the `builders: Vec<DynamicBuilder>` creation to the _top_ of the function, before the stream reading loop.

- [ ] **Task 2: Initialize State Variables**

    - Create a `leftover_buffer: Vec<u8> = Vec::new();` to hold partial row data between chunks.

    - Create a `is_header_parsed: bool = false;` flag.

    - Create a `rows_processed: usize = 0;` counter.

- [ ] **Task 3: Refactor Finalization**

    - Move the "Finalize the Arrays" and "Build the Final RecordBatch" logic (Steps 4 and 5 in the old parser) to the _very end_ of the function, after the main stream loop.


### Phase 2: Implement the Streaming Loop

This phase rewrites the core `while let Some...` loop to parse _in-place_.

- [ ] **Task 4: Prepare the Chunk Buffer**

    - Inside the `while let Some(segment_result) = stream.next().await` loop:

    - Create a _new_ `current_chunk: Vec<u8>` by combining `leftover_buffer` + the new `segment` (from `segment.extend_from_slice(...)`).

    - Clear the `leftover_buffer` (`leftover_buffer.clear()`).

- [ ] **Task 5: Create a Cursor for the Chunk**

    - Create a `cursor = Cursor::new(&current_chunk)`.

- [ ] **Task 6: Handle the Header (Once)**

    - Add an `if !is_header_parsed` block.

    - Inside this block, parse the 19-byte Postgres header (magic signature, flags) using the `cursor`.

    - If the header is incomplete (hits `UnexpectedEof`), copy the _entire_ `current_chunk` back into `leftover_buffer` and `continue` to the next segment.

    - If successful, set `is_header_parsed = true`.


### Phase 3: Implement the Inner Parsing Loop (The Core Logic)

This is the most complex part: parsing rows until the chunk runs out of data.

- [ ] **Task 7: Create the Inner `loop`**

    - Inside the `while` loop (after the header check), create a new, inner `loop { ... }`. This loop will repeatedly try to parse rows from the `cursor`.

- [ ] **Task 8: Implement Safe Read (Handling Partial Data)**

    - Before attempting to read a row, save the current position: `let safe_position = cursor.position();`.

    - Attempt to read the row's 2-byte column count (`read_i16`).

    - **If it fails with `UnexpectedEof`:**

        - This chunk is finished, but the data is partial.

        - Copy the remaining bytes from `safe_position` to the end of `current_chunk` into `leftover_buffer`.

        - `break;` the _inner_ loop (to go get the next network segment).

    - **If it succeeds:**

        - Check for the `-1` trailer (end of stream). If found, `break` _both_ loops.

        - Increment `rows_processed`.

- [ ] **Task 9: Implement Streaming Field Parsing**

    - Inside the `for (i, builder) in builders.iter_mut().enumerate()` loop:

    - Save the position _again_ (`let field_safe_pos = cursor.position();`).

    - Attempt to read the 4-byte field length.

    - Attempt to read the field data (e.g., `read_i64`).

    - **If** _**any**_ **read fails with `UnexpectedEof`:**

        - The row is partial and split across chunks.

        - Restore the cursor: `cursor.set_position(safe_position);` (rewind to the start of the row).

        - Copy _all_ remaining bytes from `safe_position` into `leftover_buffer`.

        - `break;` the _inner_ loop.

    - **If it succeeds:**

        - Append the value to the correct Arrow builder.