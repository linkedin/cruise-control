from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractNonNegativeIntegerParameter(AbstractParameter):
    def __init__(self, value: int):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != int:
            raise ValueError(f"{self.value} is not an integer value")
        elif self.value < 0:
            raise ValueError(f"{self.value} must be a non-negative integer")


class ReviewIDParameter(AbstractNonNegativeIntegerParameter):
    """review_id=[id]"""
    name = 'review_id'
    description = 'The id of the approved review'
    argparse_properties = {
        'args': ('--review-id',),
        'kwargs': dict(metavar='K', help=description, type=int)
    }