from typing import Union

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter

set_of_choices = {'true', 'false'}


class AbstractBooleanParameter(AbstractParameter):
    def __init__(self, value: Union[bool, str]):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != bool and type(self.value) != str:
            raise ValueError(f"{self.value} is neither a boolean nor a string")


class AllowCapacityEstimationParameter(AbstractBooleanParameter):
    """allow_capacity_estimation=[true/false]"""
    name = 'allow_capacity_estimation'
    description = "Whether to allow capacity estimation when cruise-control is unable to obtain all per-broker capacity information"
    argparse_properties = {
        'args': ('--allow-capacity-estimation',),
        # Presume that the sensible default is to NOT allow capacity estimation
        'kwargs': dict(help=description, action='store_true')
    }


class ClearMetricsParameter(AbstractBooleanParameter):
    """clearmetrics=[true/false]"""
    name = 'clearmetrics'
    description = 'Clear the existing metric samples'
    argparse_properties = {
        'args': ('--clearmetrics', '--clear-metrics'),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class DryRunParameter(AbstractBooleanParameter):
    """dryRun=[true/false]"""
    # Docstrings list 'dryRun', but DRY_RUN_PARAM is 'dryrun'
    name = 'dryrun'
    description = 'Calculate, but do not execute, the cruise-control proposal'
    argparse_properties = {
        'args': ('-n', '--dry-run', '--dryrun'),
        # Presume that the lack of --dryrun affirmatively means no dry run
        'kwargs': dict(help=description, action='store_true')
    }


class ExcludeFollowerDemotionParameter(AbstractBooleanParameter):
    """exclude_follower_demotion=[true/false]"""
    name = 'exclude_follower_demotion'
    description = 'Whether to operate on partitions which only have follower replicas on the specified broker(s)'
    argparse_properties = {
        'args': ('--exclude-follower-demotion',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class ExcludeRecentlyDemotedBrokersParameter(AbstractBooleanParameter):
    """exclude_recently_demoted_brokers=[true/false]"""
    name = 'exclude_recently_demoted_brokers'
    description = 'Whether to exclude all recently-demoted brokers from this endpoint\'s action'
    argparse_properties = {
        'args': ('--exclude-recently-demoted-brokers', '--exclude-recently-demoted', '--exclude-demoted'),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class ExcludeRecentlyRemovedBrokersParameter(AbstractBooleanParameter):
    """exclude_recently_removed_brokers=[true/false]"""
    name = 'exclude_recently_removed_brokers'
    description = 'Whether to exclude all recently-removed brokers from this endpoint\'s action'
    argparse_properties = {
        'args': ('--exclude-recently-removed-brokers', '--exclude-recently-removed', '--exclude-removed'),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class ForceStopParameter(AbstractBooleanParameter):
    """force_stop=[true/false]"""
    name = 'force_stop'
    description = 'Whether to make cruise-control trigger a ' \
                  'kafka controller switch to clear ongoing partition movements'
    argparse_properties = {
        'args': ('--force-stop', '--force_stop'),
        # Presume that the lack of --force-stop affirmatively means no force stop
        'kwargs': dict(help=description, action='store_true')
    }


class IgnoreProposalCacheParameter(AbstractBooleanParameter):
    """ignore_proposal_cache=[true/false]"""
    name = 'ignore_proposal_cache'
    description = 'Whether to ignore the proposal cache'
    argparse_properties = {
        'args': ('--ignore-proposal-cache',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class JSONParameter(AbstractBooleanParameter):
    """json=[true/false]"""
    name = 'json'
    description = "Whether cruise-control's response should be in JSON format"
    argparse_properties = {
        'args': ('--json', '--json-response'),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class MaxLoadParameter(AbstractBooleanParameter):
    """max_load=[true/false]"""
    name = 'max_load'
    description = "Whether to return the peak load (max load) or the average load"
    argparse_properties = {
        'args': ('--max-load',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class SkipHardGoalCheckParameter(AbstractBooleanParameter):
    """skip_hard_goal_check=[true/false]"""
    name = 'skip_hard_goal_check'
    description = "Whether cruise-control should skip all hard goal checks"
    argparse_properties = {
        'args': ('--skip-hard-goal-check', '--skip-hard-goal-checks'),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class SkipRackAwarenessCheckParameter(AbstractBooleanParameter):
    """skip_rack_awareness_check=[true/false]"""
    name = 'skip_rack_awareness_check'
    description = "Whether to skip the rack-awareness check when performing this action"
    argparse_properties = {
        'args': ('--skip-rack-awareness-check',),
        # Presume that the sensible default is to NOT skip rack awareness
        'kwargs': dict(help=description, action='store_true')
    }


class SkipURPDemotionParameter(AbstractBooleanParameter):
    """skip_urp_demotion=[true/false]"""
    name = 'skip_urp_demotion'
    description = 'Whether to operate on partitions which are currently under-replicated'
    argparse_properties = {
        'args': ('--skip-urp-demotion',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class StopOngoingExecutionParameter(AbstractBooleanParameter):
    """stop_ongoing_execution=[true/false]"""
    name = 'stop_ongoing_execution'
    description = "Whether to stop the ongoing execution (if any) and start executing the given request"
    argparse_properties = {
        'args': ('--stop-ongoing-execution',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class SuperVerboseParameter(AbstractBooleanParameter):
    """super_verbose=[true/false]"""
    name = 'super_verbose'
    description = "Whether cruise-control's response should return super-verbose information"
    argparse_properties = {
        'args': ('--super-verbose',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class ThrottleRemovedBrokerParameter(AbstractBooleanParameter):
    """throttle_removed_broker=[true/false]"""
    name = 'throttle_removed_broker'
    description = "Whether to apply the concurrent_partition_movements_per_broker limitation to the removed broker"
    argparse_properties = {
        'args': ('--throttle-removed-broker',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class UseReadyDefaultGoalsParameter(AbstractBooleanParameter):
    """use_ready_default_goals=[true/false]"""
    name = 'use_ready_default_goals'
    description = "Whether cruise-control should use its ready default goals"
    argparse_properties = {
        'args': ('--use-ready-default-goals',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }


class VerboseParameter(AbstractBooleanParameter):
    """verbose=[true/false]"""
    name = 'verbose'
    description = 'Whether cruise-control\'s response should return verbose information'
    argparse_properties = {
        'args': ('--verbose',),
        # There is no sensible default here; allow for ternary state
        'kwargs': dict(help=description, choices=set_of_choices, metavar='BOOL', type=str.lower)
    }
