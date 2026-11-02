import pytest

from dataregistry.api.rate_limit import FailedLookupLimiter


class FakeClock:
    """A settable monotonic clock, so window arithmetic is tested without sleeping."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


def _limiter(clock, **kwargs):
    kwargs.setdefault('max_failures', 3)
    kwargs.setdefault('window_seconds', 60.0)
    return FailedLookupLimiter(clock=clock, **kwargs)


def test_defaults_allow_nineteen_failures_and_block_at_twenty_within_a_minute():
    limiter = FailedLookupLimiter()
    for _ in range(19):
        limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') == 0

    limiter.record_failure('10.0.0.1')
    assert 1 <= limiter.retry_after('10.0.0.1') <= 60


def test_unknown_client_is_allowed(clock):
    assert _limiter(clock).retry_after('10.0.0.1') == 0


def test_allowed_below_the_threshold(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') == 0


def test_blocked_at_exactly_max_failures(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') >= 1


def test_retry_after_is_seconds_until_the_oldest_failure_expires(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')   # t=1000, expires at 1060
    clock.advance(10)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.1')

    assert limiter.retry_after('10.0.0.1') == 50
    clock.advance(20)
    assert limiter.retry_after('10.0.0.1') == 30


def test_retry_after_rounds_up_to_whole_seconds(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')

    clock.advance(0.25)
    assert limiter.retry_after('10.0.0.1') == 60   # 59.75 left
    clock.advance(58.5)
    assert limiter.retry_after('10.0.0.1') == 2    # 1.25 left


def test_retry_after_is_at_least_one_second(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')

    clock.advance(59.999)   # a thousandth of a second left
    assert limiter.retry_after('10.0.0.1') == 1


def test_unblocks_once_the_window_has_passed(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') > 0

    clock.advance(59.5)
    assert limiter.retry_after('10.0.0.1') > 0
    clock.advance(0.5)   # failures are exactly window_seconds old: expired
    assert limiter.retry_after('10.0.0.1') == 0


def test_window_slides_as_old_failures_expire(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')   # t=1000
    clock.advance(30)
    limiter.record_failure('10.0.0.1')   # t=1030
    clock.advance(25)
    limiter.record_failure('10.0.0.1')   # t=1055 -> third live failure, blocked
    assert limiter.retry_after('10.0.0.1') == 5

    clock.advance(5)                     # t=1060: only the first failure expired
    assert limiter.retry_after('10.0.0.1') == 0
    limiter.record_failure('10.0.0.1')   # two live (1030, 1055) + this one: blocked again
    assert limiter.retry_after('10.0.0.1') == 30   # the 1030 failure expires at 1090


def test_expired_failures_do_not_count_toward_the_threshold(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.1')
    clock.advance(61)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') == 0


def test_checking_a_blocked_client_does_not_extend_the_block(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')

    for _ in range(10):
        clock.advance(5)
        assert limiter.retry_after('10.0.0.1') > 0   # polling while blocked records nothing

    clock.advance(10)   # 60s after the original failures
    assert limiter.retry_after('10.0.0.1') == 0


def test_failures_recorded_while_blocked_do_not_extend_the_block(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')   # t=1000
    clock.advance(10)
    limiter.record_failure('10.0.0.1')   # t=1010
    clock.advance(10)
    limiter.record_failure('10.0.0.1')   # t=1020 -> blocked until the t=1000 failure expires
    clock.advance(10)
    limiter.record_failure('10.0.0.1')   # t=1030: e.g. a request that raced past the check

    assert limiter.retry_after('10.0.0.1') == 30
    clock.advance(30)   # t=1060: the original block is over; the raced failure was not kept
    assert limiter.retry_after('10.0.0.1') == 0


def test_clients_are_isolated(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.2')

    assert limiter.retry_after('10.0.0.1') > 0
    assert limiter.retry_after('10.0.0.2') == 0
    assert limiter.retry_after('10.0.0.3') == 0


def test_a_client_with_no_live_failures_is_dropped(clock):
    limiter = _limiter(clock)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.2')
    assert len(limiter._clients) == 2

    clock.advance(61)
    limiter.retry_after('10.0.0.1')
    assert len(limiter._clients) == 1   # .1 pruned on access; .2 not touched yet
    limiter.record_failure('10.0.0.2')
    assert len(limiter._clients) == 1   # stale entry pruned, one fresh failure recorded


def test_retry_after_does_not_start_tracking_unknown_clients(clock):
    limiter = _limiter(clock)
    limiter.retry_after('10.0.0.1')
    assert len(limiter._clients) == 0


def test_tracked_clients_are_bounded_and_least_recently_seen_is_evicted(clock):
    limiter = _limiter(clock, max_tracked_clients=3)
    for i in range(1, 6):
        limiter.record_failure(f'10.0.0.{i}')
        assert len(limiter._clients) <= 3

    assert len(limiter._clients) == 3
    # 1 and 2 were evicted (least recently seen); their failures are forgotten
    for _ in range(2):
        limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') == 0   # started over from zero, not 1 + 2
    # 4 and 5 survived with their single failure each
    limiter.record_failure('10.0.0.5')
    limiter.record_failure('10.0.0.5')
    assert limiter.retry_after('10.0.0.5') > 0


def test_recently_seen_clients_survive_eviction(clock):
    limiter = _limiter(clock, max_tracked_clients=3)
    for i in range(1, 4):
        limiter.record_failure(f'10.0.0.{i}')
    limiter.record_failure('10.0.0.1')   # 1 is now the most recently seen
    limiter.record_failure('10.0.0.4')   # evicts 2, the least recently seen

    assert len(limiter._clients) == 3
    limiter.record_failure('10.0.0.1')   # third failure for 1: it was not evicted
    assert limiter.retry_after('10.0.0.1') > 0
    limiter.record_failure('10.0.0.2')
    limiter.record_failure('10.0.0.2')
    assert limiter.retry_after('10.0.0.2') == 0   # 2 was evicted: only two failures on record


def test_checking_a_client_counts_as_seeing_it(clock):
    limiter = _limiter(clock, max_tracked_clients=2)
    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.2')
    limiter.retry_after('10.0.0.1')      # refreshes 1, leaving 2 least recently seen
    limiter.record_failure('10.0.0.3')   # evicts 2

    limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.1')
    assert limiter.retry_after('10.0.0.1') > 0   # 1 kept its first failure
    limiter.record_failure('10.0.0.2')
    limiter.record_failure('10.0.0.2')
    assert limiter.retry_after('10.0.0.2') == 0  # 2 lost its first failure


def test_reset_clears_all_state(clock):
    limiter = _limiter(clock)
    for _ in range(3):
        limiter.record_failure('10.0.0.1')
    limiter.record_failure('10.0.0.2')
    assert limiter.retry_after('10.0.0.1') > 0

    limiter.reset()

    assert limiter.retry_after('10.0.0.1') == 0
    assert len(limiter._clients) == 0
