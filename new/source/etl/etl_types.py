# new/source/etl/etl_types.py

from typing import Any, Dict, List, Tuple, Union, Callable
# Import DataFrame from its location
from ..utils.dataframe import DataFrame 

# --- Type Definitions for Payloads and Tasks ---

# Payload for an extraction task (input to ExtractStage)
ExtractionTaskPayload = Dict[str, Any]
# Full task item for extraction (input to ExtractStage queue)
ExtractionTaskItem = Tuple[str, ExtractionTaskPayload]

# --- Type Definitions for Queue Items (as tuples) ---

# Item carrying just a DataFrame (e.g., Output from TransformStage)
DataFrameQueueItem = Tuple[str, DataFrame]

# Item carrying raw data and payload (e.g., Output from ExtractStage for Orders)
# NOTE: ExtractStage now outputs (task_id, (df, payload)) via ThreadWrapper
ExtractedDataQueueItem = Tuple[str, Tuple[DataFrame, ExtractionTaskPayload]]

# Item carrying processed orders (Output from OrderProcessingStage)
# NOTE: OrderProcessingStage now outputs (task_id, (data_dict, payload)) via ThreadWrapper
ProcessedDataQueueItem = Tuple[str, Tuple[Dict[str, DataFrame], ExtractionTaskPayload]]

# Item type that the LoadStage input queue will receive.
# It can receive simple DataFrames (from Transform/Dispatch) or processed orders.
LoadStageInputItem = Union[DataFrameQueueItem, ProcessedDataQueueItem]

# --- Sentinel Type ---
# Used to signal the end of processing in queues.
Sentinel = type(None)