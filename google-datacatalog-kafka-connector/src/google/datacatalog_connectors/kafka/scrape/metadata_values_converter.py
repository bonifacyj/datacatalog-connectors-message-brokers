#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dateutil.relativedelta import relativedelta


class MetadataValuesConverter:
    '''
    Duration-related values in Kafka, such as retention time,
    are stored in ms, which might not always be human readable.
    The same goes for space values, that Kafka stores in bytes.
    This class converts those time and space values to
    human-readable format.
    '''

    @staticmethod
    def get_human_readable_duration_value(duration_ms):
        '''
        :param duration_ms: int or string, in milliseconds
        :return: duration string in a shape
        X y X d X h X min etc
        '''
        duration_ms = int(duration_ms)
        duration_in_microseconds = duration_ms * 1000
        duration_time = _Relativedelta(microseconds=duration_in_microseconds)
        human_readable_duration = ''
        if duration_time.years > 0:
            human_readable_duration += '{}y'.format(duration_time.years)
        if duration_time.days > 0:
            human_readable_duration += ' {}d'.format(duration_time.days)
        if duration_time.hours > 0:
            human_readable_duration += ' {}h'.format(duration_time.hours)
        if duration_time.minutes > 0:
            human_readable_duration += ' {}min'.format(duration_time.minutes)
        if duration_time.seconds > 0:
            human_readable_duration += ' {}sec'.format(duration_time.seconds)
        if duration_time.microseconds > 0:
            human_readable_duration += ' {}ms'.format(
                duration_time.microseconds / 1000)
        return human_readable_duration.strip()

    @staticmethod
    def get_human_readable_size_value(size_bytes):
        '''
        :param size_val: int or string, in bytes
        :return: human-readable size
        '''
        size_val = int(size_bytes)
        units = ['bytes', 'KB', 'MB', 'GB']
        for unit in units:
            if size_val < 1024.0:
                human_readable_space = '{} {}'.format(size_val, unit)
                return human_readable_space
            size_val = size_val / 1024.0
        return '{} TB'.format(size_val)


class _Relativedelta(relativedelta):
    '''
    Native relativedelta does not transform
    duration with days > 365 into years and days.
    This extension does that.
    '''

    def _fix_with_years(self):
        days_per_year = 365
        if self.days > days_per_year:
            self.years, self.days = divmod(self.days, days_per_year)

    def _fix(self):
        super()._fix()
        self._fix_with_years()
