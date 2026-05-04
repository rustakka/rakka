//! BidiFlow — composed of two flows in opposite directions. akka.net: `BidiFlow`.

use crate::flow::Flow;

pub struct BidiFlow<In1, Out1, In2, Out2> {
    pub forward: Flow<In1, Out1>,
    pub backward: Flow<In2, Out2>,
}

impl<In1, Out1, In2, Out2> BidiFlow<In1, Out1, In2, Out2>
where
    In1: Send + 'static,
    Out1: Send + 'static,
    In2: Send + 'static,
    Out2: Send + 'static,
{
    pub fn from_flows(forward: Flow<In1, Out1>, backward: Flow<In2, Out2>) -> Self {
        Self { forward, backward }
    }
}
