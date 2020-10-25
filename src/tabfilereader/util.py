#
# Copyright (c) 2020, Jason Simeone
#

import re
import sys


if sys.version_info >= (3, 7):
    RegexType = re.Pattern
else:
    RegexType = re._pattern_type  # noqa

