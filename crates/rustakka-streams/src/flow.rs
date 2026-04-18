//! Flow — a linear transformation from `In` to `Out`.

use futures::stream::{BoxStream, StreamExt};

pub struct Flow<In, Out> {
    pub(crate) transform:
        Box<dyn FnOnce(BoxStream<'static, In>) -> BoxStream<'static, Out> + Send + 'static>,
}

impl<In: Send + 'static, Out: Send + 'static> Flow<In, Out> {
    pub fn from_fn<F>(mut f: F) -> Self
    where
        F: FnMut(In) -> Out + Send + 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| s.map(move |x| f(x)).boxed()),
        }
    }

    pub fn filter<F>(mut f: F) -> Flow<In, In>
    where
        F: FnMut(&In) -> bool + Send + 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                s.filter(move |v| futures::future::ready(f(v))).boxed()
            }),
        }
    }

    pub fn then<Out2, F>(self, mut g: F) -> Flow<In, Out2>
    where
        Out2: Send + 'static,
        F: FnMut(Out) -> Out2 + Send + 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                let out = (self.transform)(s);
                out.map(move |x| g(x)).boxed()
            }),
        }
    }
}
