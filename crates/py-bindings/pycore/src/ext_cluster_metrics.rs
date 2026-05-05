//! Cluster-metrics submodule..
//!
//! Exposes the EWMA smoother, the metrics-selector enum used to weight
//! routees, the `WeightedRoutees` picker, the metrics PDU and the
//! `apply_metrics_pdu` reducer. The lower-layer `ClusterMetrics` /
//! `AdaptiveLoadBalancer` types are re-exposed too for completeness.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;

use atomr_cluster_metrics::{
    apply_metrics_pdu, AdaptiveLoadBalancer, ClusterMetrics, Ewma, MetricsPdu, MetricsSelector, NodeMetrics,
    WeightedRoutees,
};

#[pyclass(name = "NodeMetrics", module = "atomr._native.cluster_metrics")]
#[derive(Clone)]
pub struct PyNodeMetrics {
    pub(crate) inner: NodeMetrics,
}

#[pymethods]
impl PyNodeMetrics {
    #[new]
    #[pyo3(signature = (address, timestamp=0, cpu_load=0.0, memory_used=0, memory_max=0))]
    fn new(address: String, timestamp: u64, cpu_load: f64, memory_used: u64, memory_max: u64) -> Self {
        Self { inner: NodeMetrics { address, timestamp, cpu_load, memory_used, memory_max } }
    }

    #[getter]
    fn address(&self) -> String {
        self.inner.address.clone()
    }
    #[getter]
    fn timestamp(&self) -> u64 {
        self.inner.timestamp
    }
    #[getter]
    fn cpu_load(&self) -> f64 {
        self.inner.cpu_load
    }
    #[getter]
    fn memory_used(&self) -> u64 {
        self.inner.memory_used
    }
    #[getter]
    fn memory_max(&self) -> u64 {
        self.inner.memory_max
    }
    fn memory_usage(&self) -> f64 {
        self.inner.memory_usage()
    }
}

#[pyclass(name = "ClusterMetrics", module = "atomr._native.cluster_metrics")]
pub struct PyClusterMetrics {
    pub(crate) inner: Arc<ClusterMetrics>,
}

#[pymethods]
impl PyClusterMetrics {
    #[new]
    fn new() -> Self {
        Self { inner: Arc::new(ClusterMetrics::new()) }
    }

    fn publish(&self, metrics: &PyNodeMetrics) {
        self.inner.publish(metrics.inner.clone());
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for m in self.inner.snapshot() {
            list.append(Py::new(py, PyNodeMetrics { inner: m })?)?;
        }
        Ok(list.unbind())
    }

    fn get(&self, py: Python<'_>, address: String) -> PyResult<Option<Py<PyNodeMetrics>>> {
        match self.inner.get(&address) {
            Some(m) => Ok(Some(Py::new(py, PyNodeMetrics { inner: m })?)),
            None => Ok(None),
        }
    }

    fn node_count(&self) -> usize {
        self.inner.node_count()
    }
}

/// Exponentially-weighted moving average. 
/// ``.
#[pyclass(name = "Ewma", module = "atomr._native.cluster_metrics")]
pub struct PyEwma {
    inner: Mutex<Ewma>,
}

#[pymethods]
impl PyEwma {
    #[new]
    #[pyo3(signature = (initial, alpha))]
    fn new(initial: f64, alpha: f64) -> Self {
        Self { inner: Mutex::new(Ewma::new(initial, alpha)) }
    }

    /// Construct from a half-life expressed as samples.
    #[staticmethod]
    fn from_half_life(initial: f64, half_life_samples: f64) -> Self {
        Self { inner: Mutex::new(Ewma::from_half_life(initial, half_life_samples)) }
    }

    fn update(&self, sample: f64) -> f64 {
        self.inner.lock().update(sample)
    }

    #[getter]
    fn value(&self) -> f64 {
        self.inner.lock().value
    }

    #[getter]
    fn alpha(&self) -> f64 {
        self.inner.lock().alpha
    }
}

/// Selector identifying which dimension drives the
/// [`WeightedRoutees`] picker. Strings: `"cpu"`, `"heap"`, `"mix"`.
#[pyclass(name = "MetricsSelector", module = "atomr._native.cluster_metrics")]
#[derive(Clone)]
pub struct PyMetricsSelector {
    pub(crate) inner: MetricsSelector,
}

#[pymethods]
impl PyMetricsSelector {
    #[new]
    fn new(name: String) -> PyResult<Self> {
        let inner = match name.as_str() {
            "cpu" => MetricsSelector::Cpu,
            "heap" => MetricsSelector::Heap,
            "mix" => MetricsSelector::Mix,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown selector: {other:?} (expected cpu, heap, or mix)"
                )))
            }
        };
        Ok(Self { inner })
    }

    /// Compute the routing weight for a node — higher == more
    /// preferable.
    fn weight(&self, m: &PyNodeMetrics) -> f64 {
        self.inner.weight(&m.inner)
    }

    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            MetricsSelector::Cpu => "cpu",
            MetricsSelector::Heap => "heap",
            MetricsSelector::Mix => "mix",
        }
    }
}

/// Pick a routee with probability proportional to its
/// [`MetricsSelector::weight`]..
#[pyclass(name = "WeightedRoutees", module = "atomr._native.cluster_metrics")]
pub struct PyWeightedRoutees {
    inner: WeightedRoutees,
}

#[pymethods]
impl PyWeightedRoutees {
    #[new]
    fn new(metrics: &PyClusterMetrics, selector: &PyMetricsSelector) -> Self {
        Self { inner: WeightedRoutees::new(metrics.inner.clone(), selector.inner) }
    }

    /// Pick a candidate using `seed ∈ [0.0, 1.0)`.
    fn pick(&self, candidates: Vec<String>, seed: f64) -> Option<String> {
        let refs: Vec<&str> = candidates.iter().map(|s| s.as_str()).collect();
        self.inner.pick(&refs, seed).map(|s| s.to_string())
    }
}

/// Adaptive load balancer that picks the lowest-CPU candidate.
#[pyclass(name = "AdaptiveLoadBalancer", module = "atomr._native.cluster_metrics")]
pub struct PyAdaptiveLoadBalancer {
    inner: AdaptiveLoadBalancer,
}

#[pymethods]
impl PyAdaptiveLoadBalancer {
    #[new]
    fn new(metrics: &PyClusterMetrics) -> Self {
        Self { inner: AdaptiveLoadBalancer::new(metrics.inner.clone()) }
    }

    fn pick(&self, candidates: Vec<String>) -> Option<String> {
        let refs: Vec<&str> = candidates.iter().map(|s| s.as_str()).collect();
        self.inner.pick(&refs).map(|s| s.to_string())
    }
}

/// Apply an inbound metrics PDU into a `ClusterMetrics` snapshot.
/// `pdu_kind` must be `"push"` or `"push_batch"`; the value is one or
/// many `NodeMetrics` instances respectively.
#[pyfunction]
fn apply_pdu(metrics: &PyClusterMetrics, pdu_kind: String, samples: Vec<PyRef<'_, PyNodeMetrics>>) -> PyResult<()> {
    let owned: Vec<NodeMetrics> = samples.into_iter().map(|m| m.inner.clone()).collect();
    let pdu = match pdu_kind.as_str() {
        "push" => {
            if owned.len() != 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "push pdu requires exactly one NodeMetrics sample",
                ));
            }
            MetricsPdu::Push(owned.into_iter().next().unwrap())
        }
        "push_batch" => MetricsPdu::PushBatch(owned),
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown pdu kind: {other:?} (expected push or push_batch)"
            )))
        }
    };
    apply_metrics_pdu(&metrics.inner, pdu);
    Ok(())
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster_metrics")?;
    sub.add_class::<PyNodeMetrics>()?;
    sub.add_class::<PyClusterMetrics>()?;
    sub.add_class::<PyEwma>()?;
    sub.add_class::<PyMetricsSelector>()?;
    sub.add_class::<PyWeightedRoutees>()?;
    sub.add_class::<PyAdaptiveLoadBalancer>()?;
    sub.add_function(wrap_pyfunction!(apply_pdu, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
