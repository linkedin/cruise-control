# Flatten the imports so that packages above just need:
# from CCParameter import {SomeConcreteParameterClass}
from cruisecontrolclient.client.CCParameter.BooleanParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.CommaSeparatedParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.NonNegativeIntegerParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.Parameter import *  # noqa
from cruisecontrolclient.client.CCParameter.PositiveIntegerParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.RegularExpressionParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.SetOfChoicesParameter import *  # noqa
from cruisecontrolclient.client.CCParameter.TimeStampParameter import *  # noqa
