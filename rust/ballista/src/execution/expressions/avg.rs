// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Schema};
use crate::error::Result;
use crate::execution::physical_plan::{
    Accumulator, AggregateExpr, AggregateMode, ColumnarBatch, ColumnarValue, Expression,
};

#[derive(Debug)]
pub struct Avg {
    input: Arc<dyn Expression>,
}

impl Avg {
    pub fn new(input: Arc<dyn Expression>) -> Self {
        Self { input }
    }
}

impl AggregateExpr for Avg {
    fn name(&self) -> String {
        unimplemented!()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        unimplemented!()
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        unimplemented!()
    }

    fn evaluate_input(&self, _batch: &ColumnarBatch) -> Result<ColumnarValue> {
        unimplemented!()
    }

    fn create_accumulator(&self, _mode: &AggregateMode) -> Rc<RefCell<dyn Accumulator>> {
        unimplemented!()
    }
}

/// Create an avg expression
pub fn avg(expr: Arc<dyn Expression>) -> Arc<dyn AggregateExpr> {
    Arc::new(Avg::new(expr))
}
