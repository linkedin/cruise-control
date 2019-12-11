from cruisecontrolclient.client.CCParameter import AbstractParameter


class AbstractReasonParameter(AbstractParameter):
    def validate_value(self):
        """
        The validation strategy here is to avoid a Bobby Tables situation
        by ensuring that the provided reason does not contain any
        RFC-3986 "reserved" characters.

        See https://tools.ietf.org/html/rfc3986#section-2.2
        See also https://xkcd.com/327/
        :return:
        """
        gen_delims = set(":/?#[]@")
        sub_delims = set("!$&'()*+,;=")
        reserved_html_characters = gen_delims | sub_delims
        for character in self.value:
            if character in reserved_html_characters:
                raise ValueError(f"\"{character}\" is a reserved HTML character"
                                 " and cannot be transmitted correctly to cruise-control"
                                 " as part of the reason= parameter")


class ReasonForPauseParameter(AbstractReasonParameter):
    """reason=[reason-for-pause]"""
    name = 'reason'
    description = 'The reason for pausing or resuming the sampling'
    argparse_properties = {
        'args': ('--pause-reason',),
        'kwargs': dict(help=description, metavar='REASON', type=str)
    }


class ReasonForReviewParameter(AbstractReasonParameter):
    """reason=[reason-for-review]"""
    name = 'reason'
    description = 'The reason for the review'
    argparse_properties = {
        'args': ('--review-reason',),
        'kwargs': dict(help=description, metavar='REASON', type=str)
    }


class ReasonForRequestParameter(AbstractReasonParameter):
    """reason=[reason-for-request]"""
    name = 'reason'
    description = 'The reason for the request'
    argparse_properties = {
        'args': ('--request-reason',),
        'kwargs': dict(help=description, metavar='REASON', type=str)
    }
