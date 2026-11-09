"""Unit tests for ClassModel.normalize_datasets / primary_dataset."""

from back.objects.ontology.ClassModel import ClassModel


def test_normalize_none_and_empty():
    assert ClassModel.normalize_datasets(None) == []
    assert ClassModel.normalize_datasets({}) == []
    assert ClassModel.normalize_datasets([]) == []


def test_normalize_singular_object():
    ds = {"fullName": "a.b.c", "asset": "c"}
    assert ClassModel.normalize_datasets(ds) == [ds]


def test_normalize_class_legacy_dataset():
    cls = {"name": "Customer", "dataset": {"fullName": "a.b.c", "asset": "c"}}
    assert ClassModel.normalize_datasets(cls) == [cls["dataset"]]
    assert ClassModel.primary_dataset(cls) == cls["dataset"]


def test_normalize_class_datasets_list():
    datasets = [
        {"fullName": "a.b.c", "asset": "c"},
        {"fullName": "a.b.d", "asset": "d", "type": "METRIC_VIEW"},
    ]
    cls = {"name": "Customer", "datasets": datasets}
    assert ClassModel.normalize_datasets(cls) == datasets
    assert ClassModel.primary_dataset(cls) == datasets[0]


def test_build_class_from_data_prefers_datasets():
    datasets = [{"fullName": "a.b.c", "asset": "c"}]
    built = ClassModel.build_class_from_data(
        {"name": "Customer", "datasets": datasets},
        existing={"dataset": {"fullName": "old.x.y", "asset": "y"}},
    )
    assert built["datasets"] == datasets
    assert built["dataset"] == datasets[0]


def test_build_class_from_data_legacy_dataset_request():
    legacy = {"fullName": "a.b.c", "asset": "c"}
    built = ClassModel.build_class_from_data({"name": "Customer", "dataset": legacy})
    assert built["datasets"] == [legacy]
    assert built["dataset"] == legacy
