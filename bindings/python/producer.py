import _confluent_kafka_rust as _lib
from _confluent_kafka_rust import ProducerRecord
from concurrent.futures import (Future)


# ProducerRecord is a C extension type imported from _confluent_kafka_rust module
# It stores kafka_producer_ProducerRecord_t internally for optimized performance


class KafkaError(Exception):
    """Kafka error with code, message, and retriable/fatal flags."""

    def __init__(self):
        raise NotImplementedError()

    def __str__(self):
        return self._message

    @staticmethod
    def _from_c(_id: int):
        ret = KafkaError.__new__(KafkaError)
        ret._code = _lib.KafkaError_code(_id)
        ret._message = _lib.KafkaError_message(_id)
        ret._is_retriable = _lib.KafkaError_is_retriable(_id)
        ret._is_fatal = _lib.KafkaError_is_fatal(_id)
        _lib.KafkaError_destroy(_id)
        return ret

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        return self._message

    @property
    def is_retriable(self):
        return self._is_retriable

    @property
    def is_fatal(self):
        return self._is_fatal


class RecordMetadata:
    def __init__(self):
        raise NotImplementedError()

    def _set_attributes_from_c(self, offset: int, partition: int,
                               topic: str, timestamp: int):
        self._offset = offset
        self._partition = partition
        self._topic = topic
        self._timestamp = timestamp

    def __del__(self):
        if not self._populated and self._id != 0:
            _lib.RecordMetadata_destroy(self._id)

    def _get_record_metadata(self):
        if not self._populated:
            self._populated = True
            _lib.RecordMetadata_copy(
                self._id,
                self._set_attributes_from_c)
        return self

    @staticmethod
    def _from_c(_id: int):
        self = RecordMetadata.__new__(RecordMetadata)
        self._id = _id
        self._populated = False
        self._topic = None
        self._offset = None
        self._partition = None
        self._timestamp = None
        return self

    def offset(self):
        return self._get_record_metadata()._offset

    def topic(self):
        return self._get_record_metadata()._topic

    def partition(self):
        return self._get_record_metadata()._partition

    def timestamp(self):
        return self._get_record_metadata()._timestamp


class Producer:

    def __init__(self, auto_complete=True):
        self.futures = set()
        self.closed = False
        self.c_producer = _lib.Producer_new(auto_complete, self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _remove_future(self, future: Future):
        if future in self.futures:
            self.futures.remove(future)

    def _add_future(self, future: Future) -> Future:
        self.futures.add(future)
        future.add_done_callback(self._remove_future)
        return future

    def _cancel(self):
        while len(self.futures) > 0:
            for future in list(self.futures):
                self._remove_future(future)
                future.cancel()

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("Producer is already closed")

    def send(self, producer_record: ProducerRecord) -> Future[RecordMetadata]:
        self._check_closed()
        if producer_record is None:
            raise ValueError("producer_record cannot be None")
        if not isinstance(producer_record, ProducerRecord):
            raise TypeError(
                "producer_record must be an instance of ProducerRecord")
        ret = Future()

        def cb(result, error):
            if ret.cancelled():
                if error != 0:
                    _lib.KafkaError_destroy(error)
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
                return
            if error != 0:
                if ret.done():
                    _lib.KafkaError_destroy(error)
                    if result != 0:
                        _lib.RecordMetadata_destroy(result)
                    return
                ret.set_exception(
                    KafkaError._from_c(error)
                )
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
            else:
                if ret.done():
                    if result != 0:
                        _lib.RecordMetadata_destroy(result)
                    return
                if result != 0:
                    ret.set_result(RecordMetadata._from_c(result))
                else:
                    ret.set_result(None)

        _lib.Producer_send(self.c_producer, producer_record, cb)
        return self._add_future(ret)

    def flush(self):
        """Flush all pending records."""
        err = _lib.Producer_flush(self.c_producer)
        if err != 0:
            raise KafkaError._from_c(err)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self._cancel()
        _lib.Producer_close(self.c_producer)


class MockProducer(Producer):

    def __init__(self, auto_complete=True):
        super().__init__(auto_complete)

    def complete_next(self):
        """Complete the next pending send successfully.

        Returns:
            True if there was a pending completion, False otherwise.
        """
        return _lib.MockProducer_complete_next(self.c_producer)

    def error_next(self, error_code, error_message=None):
        """Complete the next pending send with an error.

        Args:
            error_code: Kafka error code
            error_message: Optional error message

        Returns:
            True if there was a pending completion, False otherwise.
        """
        return _lib.MockProducer_error_next(
            self.c_producer, error_code, error_message)

    def history_count(self):
        """Returns the number of successfully sent records."""
        return _lib.MockProducer_history_count(self.c_producer)

    def clear(self):
        """Clear the sent history and pending completions."""
        _lib.MockProducer_clear(self.c_producer)
