// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::{BufMut, Bytes};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: (Vec::<u16>::new()),
            data: (Vec::<u8>::with_capacity(block_size)),
            block_size: (block_size),
            first_key: (KeyVec::new()),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let keylen_value = key.raw_ref().len();
        let valuelen_value: usize = value.len();
        let entry_len: usize = 2 + keylen_value + 2 + valuelen_value + 2; //要把offset的长度也算上

        println!(
            "key:  {:?}, value:  {:?}, data.len: {:?}, entry_len: {:?}, block_size: {:?}",
            key,
            value,
            self.data.len(),
            entry_len,
            self.block_size
        );
        if !self.data.is_empty() && (self.data.len() + entry_len) > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        self.data.put_u16(keylen_value as u16); //大端序
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(valuelen_value as u16);
        self.data.put_slice(value);

        if self.data.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data, // 这里只是移动了Vec的所有权
            offsets: self.offsets,
        }
    }
}
