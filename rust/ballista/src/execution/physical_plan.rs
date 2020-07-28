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

//! Ballista Physical Plan (Experimental).
//!
//! The physical plan is a serializable data structure describing how the plan will be executed.
//!
//! It differs from the logical plan in that it deals with specific implementations of operators
//! (e.g. SortMergeJoin versus BroadcastHashJoin) whereas the logical plan just deals with an
//! abstract concept of a join.
//!
//! The physical plan also accounts for partitioning and ordering of data between operators. 1

use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::logicalplan::Expr;
use crate::datafusion::logicalplan::LogicalPlan;
use crate::datafusion::logicalplan::Operator;
use crate::datafusion::logicalplan::ScalarValue;
use crate::distributed::scheduler::ExecutionTask;
use crate::error::{ballista_error, Result};
use crate::execution::expressions::{
    add, alias, aliased_aggr, avg, col, compare, count, div, lit, max, min, mult, subtract, sum,
};
use crate::execution::operators::{
    CsvScanExec, FilterExec, HashAggregateExec, InMemoryTableScanExec, ParquetScanExec,
    ProjectionExec, ShuffleExchangeExec, ShuffleReaderExec,
};

use crate::distributed::executor::ExecutorConfig;
use async_trait::async_trait;
use uuid::Uuid;

/// Stream of columnar batches using futures
pub type ColumnarBatchStream = Arc<dyn ColumnarBatchIter>;

#[derive(Debug, Clone)]
pub struct ExecutorMeta {
    pub id: String,
    pub host: String,
    pub port: usize,
}

/// Async iterator over a stream of columnar batches
#[async_trait]
pub trait ColumnarBatchIter: Sync + Send {
    /// Get the schema for the batches produced by this iterator.
    fn schema(&self) -> Arc<Schema>;

    /// Get the next batch from the stream, or None if the stream has ended
    async fn next(&self) -> Result<Option<ColumnarBatch>>;

    /// Notify the iterator that no more results will be fetched, so that resources
    /// can be freed immediately.
    async fn close(&self) {}
}

#[async_trait]
pub trait ExecutionContext: Send + Sync {
    async fn get_executor_ids(&self) -> Result<Vec<ExecutorMeta>>;
    async fn execute_task(
        &self,
        executor_id: ExecutorMeta,
        task: ExecutionTask,
    ) -> Result<ShuffleId>;
    async fn read_shuffle(&self, shuffle_id: &ShuffleId) -> Result<Vec<ColumnarBatch>>;
    fn config(&self) -> ExecutorConfig;
}

/// Base trait for all operators
#[async_trait]
pub trait ExecutionPlan: Send + Sync {
    /// Specified the output schema of this operator.
    fn schema(&self) -> Arc<Schema>;

    /// Specifies how data is partitioned across different nodes in the cluster
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
    }

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Specifies how data is ordered in each partition
    fn output_ordering(&self) -> Option<Vec<SortOrder>> {
        None
    }

    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_ordering(&self) -> Option<Vec<Vec<SortOrder>>> {
        None
    }

    /// Get the children of this plan. Leaf nodes have no children. Unary nodes have a single
    /// child. Binary nodes have two children.
    fn children(&self) -> Vec<Arc<PhysicalPlan>> {
        vec![]
    }

    /// Runs this query against one partition returning a stream of columnar batches
    async fn execute(
        &self,
        ctx: Arc<dyn ExecutionContext>,
        partition_index: usize,
    ) -> Result<ColumnarBatchStream>;
}

pub trait Expression: Send + Sync + Debug {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a ColumnarBatch to produce a scalar or columnar result.
    fn evaluate(&self, input: &ColumnarBatch) -> Result<ColumnarValue>;
    /// Generate schema Field type for this expression
    fn to_schema_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

/// Aggregate expression that can be evaluated against a RecordBatch
pub trait AggregateExpr: Send + Sync + Debug {
    /// Get the name to use in a schema to represent the result of this expression
    fn name(&self) -> String;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate the expression being aggregated
    fn evaluate_input(&self, batch: &ColumnarBatch) -> Result<ColumnarValue>;
    /// Create an accumulator for this aggregate expression
    fn create_accumulator(&self, mode: &AggregateMode) -> Box<dyn Accumulator>;
    /// Generate schema Field type for this expression
    fn to_schema_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

/// Aggregate accumulator
pub trait Accumulator: Send + Sync {
    /// Update the accumulator based on a columnar value
    fn accumulate(&mut self, value: &ColumnarValue) -> Result<()>;
    /// Get the final value for the accumulator
    fn get_value(&self) -> Result<Option<ScalarValue>>;
}

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Execute the query with DataFusion and return the results
    InteractiveQuery { plan: LogicalPlan },
    /// Execute a query and store the results in memory
    Execute(ExecutionTask),
    /// Collect a shuffle
    FetchShuffle(ShuffleId),
}

pub type MaybeColumnarBatch = Result<Option<ColumnarBatch>>;

/// Batch of columnar data.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ColumnarBatch {
    schema: Arc<Schema>,
    columns: Vec<ColumnarValue>,
}

impl ColumnarBatch {
    pub fn from_arrow(batch: &RecordBatch) -> Self {
        let columns = batch
            .columns()
            .iter()
            .map(|c| ColumnarValue::Columnar(c.clone()))
            .collect();
        Self {
            schema: batch.schema(),
            columns,
        }
    }

    pub fn from_values(values: &[ColumnarValue]) -> Self {
        let schema = Schema::new(
            values
                .iter()
                .enumerate()
                .map(|(i, value)| Field::new(&format!("c{}", i), value.data_type().clone(), true))
                .collect(),
        );
        Self {
            schema: Arc::new(schema),
            columns: values.to_vec(),
        }
    }

    pub fn to_arrow(&self) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .iter()
            .map(|c| match c {
                ColumnarValue::Columnar(array) => Ok(array.clone()),
                ColumnarValue::Scalar(_, _) => {
                    // note that this can be implemented easily if needed
                    Err(ballista_error("Cannot convert scalar value to Arrow array"))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].len()
    }

    pub fn column(&self, index: usize) -> &ColumnarValue {
        &self.columns[index]
    }
}

macro_rules! build_literal_array {
    ($LEN:expr, $BUILDER:ident, $VALUE:expr) => {{
        let mut builder = $BUILDER::new($LEN);
        for _ in 0..$LEN {
            builder.append_value($VALUE)?;
        }
        Ok(Arc::new(builder.finish()))
    }};
}

/// A columnar value can either be a scalar value or an Arrow array.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ColumnarValue {
    Scalar(Option<ScalarValue>, usize),
    Columnar(ArrayRef),
}

impl ColumnarValue {
    pub fn len(&self) -> usize {
        match self {
            ColumnarValue::Scalar(_, n) => *n,
            ColumnarValue::Columnar(array) => array.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> &DataType {
        match self {
            ColumnarValue::Columnar(array) => array.data_type(),
            ColumnarValue::Scalar(Some(value), _) => match value {
                ScalarValue::UInt8(_) => &DataType::UInt8,
                ScalarValue::UInt16(_) => &DataType::UInt16,
                ScalarValue::UInt32(_) => &DataType::UInt32,
                ScalarValue::UInt64(_) => &DataType::UInt64,
                ScalarValue::Int8(_) => &DataType::Int8,
                ScalarValue::Int16(_) => &DataType::Int16,
                ScalarValue::Int32(_) => &DataType::Int32,
                ScalarValue::Int64(_) => &DataType::Int64,
                ScalarValue::Float32(_) => &DataType::Float32,
                ScalarValue::Float64(_) => &DataType::Float64,
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    pub fn to_arrow(&self) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Columnar(array) => Ok(array.clone()),
            ColumnarValue::Scalar(Some(value), n) => match value {
                ScalarValue::Int8(value) => build_literal_array!(*n, Int8Builder, *value),
                ScalarValue::Int16(value) => build_literal_array!(*n, Int16Builder, *value),
                ScalarValue::Int32(value) => build_literal_array!(*n, Int32Builder, *value),
                ScalarValue::Int64(value) => build_literal_array!(*n, Int64Builder, *value),
                ScalarValue::UInt8(value) => build_literal_array!(*n, UInt8Builder, *value),
                ScalarValue::UInt16(value) => build_literal_array!(*n, UInt16Builder, *value),
                ScalarValue::UInt32(value) => build_literal_array!(*n, UInt32Builder, *value),
                ScalarValue::UInt64(value) => build_literal_array!(*n, UInt64Builder, *value),
                ScalarValue::Float32(value) => build_literal_array!(*n, Float32Builder, *value),
                ScalarValue::Float64(value) => build_literal_array!(*n, Float64Builder, *value),
                ScalarValue::Utf8(value) => build_literal_array!(*n, StringBuilder, value),
                other => Err(ballista_error(&format!(
                    "Unsupported literal type {:?}",
                    other
                ))),
            },
            _ => unimplemented!(),
        }
    }
}

/// Enumeration wrapping physical plan structs so that they can be represented in a tree easily
/// and processed using pattern matching
#[derive(Clone)]
pub enum PhysicalPlan {
    /// Projection.
    Projection(Arc<ProjectionExec>),
    /// Filter a.k.a predicate.
    Filter(Arc<FilterExec>),
    /// Hash aggregate
    HashAggregate(Arc<HashAggregateExec>),
    /// Performs a shuffle that will result in the desired partitioning.
    ShuffleExchange(Arc<ShuffleExchangeExec>),
    /// Reads results from a ShuffleExchange
    ShuffleReader(Arc<ShuffleReaderExec>),
    /// Scans a partitioned Parquet data source
    ParquetScan(Arc<ParquetScanExec>),
    /// Scans a partitioned CSV data source
    CsvScan(Arc<CsvScanExec>),
    /// Scans an in-memory table
    InMemoryTableScan(Arc<InMemoryTableScanExec>),
}

impl PhysicalPlan {
    pub fn as_execution_plan(&self) -> Arc<dyn ExecutionPlan> {
        match self {
            Self::Projection(exec) => exec.clone(),
            Self::Filter(exec) => exec.clone(),
            Self::HashAggregate(exec) => exec.clone(),
            Self::ParquetScan(exec) => exec.clone(),
            Self::CsvScan(exec) => exec.clone(),
            Self::ShuffleExchange(exec) => exec.clone(),
            Self::ShuffleReader(exec) => exec.clone(),
            Self::InMemoryTableScan(exec) => exec.clone(),
        }
    }

    pub fn with_new_children(&self, new_children: Vec<Arc<PhysicalPlan>>) -> PhysicalPlan {
        match self {
            Self::HashAggregate(exec) => {
                Self::HashAggregate(Arc::new(exec.with_new_children(new_children)))
            }
            _ => unimplemented!(),
        }
    }

    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match self {
            PhysicalPlan::CsvScan(exec) => write!(
                f,
                "CsvScan: {:?}, partitions={}; projection={:?}",
                exec.path,
                exec.filenames.len(),
                exec.projection
            ),
            PhysicalPlan::ParquetScan(exec) => write!(
                f,
                "ParquetScan: {:?}, partitions={}; projection={:?}",
                exec.path,
                exec.filenames.len(),
                exec.projection
            ),
            PhysicalPlan::HashAggregate(exec) => {
                write!(
                    f,
                    "HashAggregate: mode={:?}, groupExpr={:?}, aggrExpr={:?}",
                    exec.mode, exec.group_expr, exec.aggr_expr
                )?;
                exec.child.fmt_with_indent(f, indent + 1)
            }
            PhysicalPlan::ShuffleExchange(exec) => {
                write!(f, "Shuffle: {:?}", exec.as_ref().output_partitioning())?;
                exec.as_ref().child.fmt_with_indent(f, indent + 1)
            }
            PhysicalPlan::ShuffleReader(exec) => {
                write!(f, "ShuffleReader: shuffle_id={:?}", exec.shuffle_id)
            }
            PhysicalPlan::Projection(_exec) => write!(f, "Projection:"),
            PhysicalPlan::Filter(_exec) => write!(f, "Filter:"),
            _ => write!(f, "???"),
        }
    }
}

impl fmt::Debug for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

#[derive(Debug, Clone)]
pub enum Distribution {
    UnspecifiedDistribution,
    SinglePartition,
    BroadcastDistribution,
    ClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>,
    },
    HashClusteredDistribution {
        required_num_partitions: usize,
        clustering: Vec<Expr>,
    },
    OrderedDistribution(Vec<SortOrder>),
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
}

#[derive(Debug, Clone)]
pub enum BuildSide {
    BuildLeft,
    BuildRight,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Aggregate operator modes.
#[derive(Debug, Clone)]
pub enum AggregateMode {
    /// Partial aggregation that can run in parallel per partition
    Partial,
    /// Perform final aggregation on results of partial aggregation. For example, this would
    /// produce the SUM of SUMs, or the SUMs of COUNTs.
    Final,
    /// Perform complete aggregation in one pass. This is used when there is only a single
    /// partition to operate on.
    Complete,
}

#[derive(Debug, Clone)]
pub struct SortOrder {
    child: Arc<Expr>,
    direction: SortDirection,
    null_ordering: NullOrdering,
}

#[derive(Debug, Clone)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    UnknownPartitioning(usize),
    HashPartitioning(usize, Vec<Arc<Expr>>),
}

impl Partitioning {
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            UnknownPartitioning(n) => *n,
            HashPartitioning(n, _) => *n,
        }
    }
}

/// Unique identifier for the output shuffle partition of an operator.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShuffleId {
    pub(crate) job_uuid: Uuid,
    pub(crate) stage_id: usize,
    pub(crate) partition_id: usize,
}

impl ShuffleId {
    pub fn new(job_uuid: Uuid, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_uuid,
            stage_id,
            partition_id,
        }
    }
}

pub struct ShuffleLocation {}

/// Translate a logical expression into a physical expression that can be evaluated against
/// input data.
pub fn compile_expression(expr: &Expr, input: &Schema) -> Result<Arc<dyn Expression>> {
    match expr {
        Expr::Alias(expr, name) => Ok(alias(compile_expression(expr, input)?, name)),
        Expr::Column(n) => Ok(col(*n, input.field(*n).name())),
        Expr::UnresolvedColumn(name) => Ok(col(input.index_of(name)?, name)),
        Expr::Literal(value) => Ok(lit(value.to_owned())),
        Expr::BinaryExpr { left, op, right } => {
            let l = compile_expression(left, input)?;
            let r = compile_expression(right, input)?;
            match op {
                Operator::Plus => Ok(add(l, r)),
                Operator::Minus => Ok(subtract(l, r)),
                Operator::Multiply => Ok(mult(l, r)),
                Operator::Divide => Ok(div(l, r)),
                Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Eq
                | Operator::NotEq => Ok(compare(l, op, r)),
                other => Err(ballista_error(&format!(
                    "Unsupported binary operator in compile_expression {:?}",
                    other
                ))),
            }
        }
        other => Err(ballista_error(&format!(
            "Unsupported expression in compile_expression {:?}",
            other
        ))),
    }
}

/// Translate one or more logical expressions into physical expressions that can be evaluated
/// against input data.
pub fn compile_expressions(expr: &[Expr], input: &Schema) -> Result<Vec<Arc<dyn Expression>>> {
    expr.iter().map(|e| compile_expression(e, input)).collect()
}

/// Translate a logical aggregate expression into a physical expression that can be evaluated
/// against input data.
pub fn compile_aggregate_expression(
    expr: &Expr,
    input_schema: &Schema,
) -> Result<Arc<dyn AggregateExpr>> {
    match expr {
        Expr::Alias(expr, alias) => Ok(aliased_aggr(
            compile_aggregate_expression(expr, input_schema)?,
            alias,
        )),
        Expr::AggregateFunction { name, args, .. } => match name.to_lowercase().as_ref() {
            "avg" => Ok(avg(compile_expression(&args[0], input_schema)?)),
            "count" => Ok(count(compile_expression(&args[0], input_schema)?)),
            "max" => Ok(max(compile_expression(&args[0], input_schema)?)),
            "min" => Ok(min(compile_expression(&args[0], input_schema)?)),
            "sum" => Ok(sum(compile_expression(&args[0], input_schema)?)),
            other => Err(ballista_error(&format!(
                "Unsupported aggregate function in compile_aggregate_expression '{}'",
                other
            ))),
        },
        other => Err(ballista_error(&format!(
            "Unsupported aggregate expression in compile_aggregate_expression {:?}",
            other
        ))),
    }
}

/// Translate one or more logical aggregate expressions into physical expressions that can be evaluated
/// against input data.
pub fn compile_aggregate_expressions(
    expr: &[Expr],
    input: &Schema,
) -> Result<Vec<Arc<dyn AggregateExpr>>> {
    expr.iter()
        .map(|e| compile_aggregate_expression(e, input))
        .collect()
}
