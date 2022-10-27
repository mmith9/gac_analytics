import time
import datetime
from random import random
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, initial_rate) -> None:
        self.last_call = time.time()
        self.stalls = []
        self.initial_rate = initial_rate
        self.rate_adjust = 0
        self.rate_adjust_rnd = 0
        self.stall_adjust = 0
        self.stall_adjust_rnd = 0

    def wait_rate_limit(self):
        sleeptime = self.last_call - time.time()
        sleeptime += self.initial_rate + self.rate_adjust
        sleeptime += (self.initial_rate + self.rate_adjust_rnd) * random()

        if sleeptime > 0:
            logger.debug('sleeping for %s', sleeptime)
            time.sleep(sleeptime)
        self.last_call = time.time()
        if random() < 0.05:  # occassionally required
            self.recalibrate()

    def recalibrate(self):
        now = time.time()
        last_5_min = 0
        last_15_min = 0
        last_h = 0
        for stall in reversed(self.stalls):
            if (now - stall) <= 300:
                last_5_min += 1
            if (now - stall) <= 900:
                last_15_min += 1
            if (now - stall) <= 3600:
                last_h += 1
            if (now - stall) > 3600:  # 1h expire time
                self.stalls.remove(stall)

        self.rate_adjust = round(
            last_h / 60 + last_15_min / 15 + last_5_min / 5, 3)
        self.rate_adjust_rnd = round(last_15_min/5 + last_5_min, 3)

        self.stall_adjust = round(
            last_h / 60 + last_15_min / 15 + (last_5_min - 1) * 5, 3)
        self.stall_adjust_rnd = round(
            last_h / 10 + last_15_min / 5 + last_5_min * 5, 3)

        logger.info('recalibrated rate %s %s stalls %s %s',
                    self.rate_adjust, self.rate_adjust_rnd,
                    self.stall_adjust, self.stall_adjust_rnd)

    def adaptative_stall(self) -> bool:
        now = time.time()
        self.stalls.append(now)
        if len(self.stalls) > 30:  # deadlock? drop last 5mins and continue
            while (now - self.stalls[-1] < 300) or (len(self.stalls) > 25):
                self.stalls.pop(-1)
            logger.warning(
                'too many stalls, pruning atleast 5 last and 5 last mins')
            return False
        else:
            self.recalibrate()
            sleeptime = self.stall_adjust + self.stall_adjust_rnd * random()
            logger.info('adaptative stalling for %s', sleeptime)
            time.sleep(sleeptime)
            return True


class RateCounter:
    def __init__(self) -> None:
        self.activity_start = time.time()
        self.requests_served = 0
        self.requests = 0
        self.requests_failed = 0
        self.requests_errored = 0

    def request_served(self):
        self.requests += 1
        self.requests_served += 1

    def request_failed(self):
        self.requests += 1
        self.requests_failed += 1

    def request_errored(self):
        self.requests += 1
        self.requests_errored += 1

    def log_rates(self):
        now = time.time()
        time_passed = now - self.activity_start

        info = 'T: ' + str(datetime.timedelta(seconds=time_passed))

        info += ' All ' + str(self.requests)
        if self.requests:
            rate = self.requests / time_passed
            if rate < 0.5:
                rate = round(1 / rate, 2)
                info += ' ' + str(rate) + 's/1'
            else:
                rate = round(rate, 2)
                info += ' ' + str(rate) + '/s'

        info += ' OK ' + str(self.requests_served)
        if self.requests_served:
            rate = self.requests_served / time_passed
            if rate < 0.5:
                rate = round(1 / rate, 2)
                info += ' ' + str(rate) + 's/1'
            else:
                rate = round(rate, 2)
                info += ' ' + str(rate) + '/s'

        info += ' Fail ' + str(self.requests_failed)
        if self.requests_failed:
            rate = self.requests_failed / time_passed
            if rate < 0.5:
                rate = round(1 / rate, 2)
                info += ' ' + str(rate) + 's/1'
            else:
                rate = round(rate, 2)
                info += ' ' + str(rate) + '/s'

        info += ' ERR ' + str(self.requests_errored)
        if self.requests_errored:
            rate = self.requests_errored / time_passed
            if rate < 0.5:
                rate = round(1 / rate, 2)
                info += ' ' + str(rate) + 's/1'
            else:
                rate = round(rate, 2)
                info += ' ' + str(rate) + '/s'

        logger.info(info)
