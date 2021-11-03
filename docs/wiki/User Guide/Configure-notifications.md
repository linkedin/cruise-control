# Notifications

**Desc**: The self-healing and anomaly detection is the crown feature of Cruise Control, but often the actions taken by Cruise Control provide less visibility to its operation. Adding a slack notification for all the anomaly detection and self-healing will increase Cruise Control visibility.

## Slack

-- Excerpt from [PR#629](https://github.com/linkedin/cruise-control/pull/629) by [ananthdurai](https://github.com/ananthdurai)

**Changelog**: The SlackSelfHealingNotifier extends SelfHealingNotifier and merely override the alerts method to send the notification to Slack.

**Configuration**: The following property requires to be configured on `cruisecontrol.properties`

`slack.self.healing.notifier.webhook` (Required)
(Slack webhook URL) Ref:https://api.slack.com/incoming-webhooks
`slack.self.healing.notifier.channel` (Required)
(Slack channel name to send the alert, (e.g) `#alerts-cruise-control`
`slack.self.healing.notifier.user` (Optional)
User name to display in the slack notification message. (default: Cruise Control)
`slack.self.healing.notifier.icon` (Optional)
Icon to display in the slack notification message. (default: `information_source` )
To enable Slack notification, set `anomaly.notifier.class=com.linkedin.kafka.cruisecontrol.detector.notifier.SlackSelfHealingNotifier`

## MS Teams

**Changelog**: The MSTeamsSelfHealingNotifier extends SelfHealingNotifier and merely override the alerts method to send the notification to MS Teams.

**Configuration**: The following property requires to be configured on `cruisecontrol.properties`

`msteams.self.healing.notifier.webhook` (Required)
(MSTeams webhook URL) Ref:https://docs.microsoft.com/en-us/microsoftteams/office-365-custom-connectors
To enable MSTeams notification, set `anomaly.notifier.class=com.linkedin.kafka.cruisecontrol.detector.notifier.MSTeamsSelfHealingNotifier`

## Alerta.io

-- Excerpt from [PR#1510](https://github.com/linkedin/cruise-control/pull/1510) by [jrevillard](https://github.com/jrevillard)

**Changelog**: The AlertaSelfHealingNotifier extends SelfHealingNotifier and merely override the alerts method to send the notification to [Alerta](https://alerta.io).

**Configuration**: The following property requires to be configured on `cruisecontrol.properties`

`alerta.self.healing.notifier.api.url` (Required)
Alerta API base url (see: https://docs.alerta.io/en/latest/api/reference.html)
`alerta.self.healing.notifier.api.key` (Required)
API key used to authenticate, (see: https://docs.alerta.io/en/latest/authentication.html#api-keys)
`alerta.self.healing.notifier.environment` (Optional)
Environment attribute used to namespace the alert resource. (See: https://docs.alerta.io/en/latest/conventions.html#environments-services) (default: Alerta API default value)

To enable Alerta notification, set `anomaly.notifier.class=com.linkedin.kafka.cruisecontrol.detector.notifier.AlertaSelfHealingNotifier`
