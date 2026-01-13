from zoopipe import MultiThreadExecutor, SingleThreadExecutor


def test_single_thread_executor_default():
    executor = SingleThreadExecutor()
    assert executor.get_batch_size() == 1000


def test_single_thread_executor_custom_batch_size():
    executor = SingleThreadExecutor(batch_size=500)
    assert executor.get_batch_size() == 500


def test_multi_thread_executor_default():
    executor = MultiThreadExecutor()
    assert executor.get_batch_size() == 1000


def test_multi_thread_executor_custom_batch_size():
    executor = MultiThreadExecutor(max_workers=4, batch_size=2000)
    assert executor.get_batch_size() == 2000


def test_multi_thread_executor_max_workers():
    executor = MultiThreadExecutor(max_workers=8)
    assert executor.get_batch_size() == 1000
