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

mod builder;
mod iterator;

use crate::key::KeyVec;
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let total_size = self.data.len() + self.offsets.len() * 2 + 2;
        let mut buf = Vec::with_capacity(total_size);

        // 1. 数据区域
        buf.put_slice(&self.data);

        // 2. 偏移量数组
        for &offset in &self.offsets {
            buf.put_u16(offset); // 自动使用大端序
        }

        // 3. 偏移量数量,即元素数量
        buf.put_u16(self.offsets.len() as u16);

        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let last_two = &data[data.len() - 2..];
        let offsetlen = u16::from_be_bytes([last_two[0], last_two[1]]);

        let u16_vec: Vec<u16> = data
            .chunks(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect();
        let datalen = data.len() - (offsetlen as usize) * 2 - 2;

        Self {
            data: data[..datalen].to_vec(),
            offsets: u16_vec[datalen / 2..(data.len() / 2 - 1)].to_vec(),
        }
    }

    pub fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        let key_len = buf.get_u16() as usize;
        let key = &buf[..key_len];
        KeyVec::from_vec(key.to_vec())
    }
}
