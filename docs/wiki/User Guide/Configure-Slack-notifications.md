-- Excerpt from [PR#629](https://github.com/linkedin/cruise-control/pull/629) by [ananthdurai](https://github.com/ananthdurai)

**Desc**: The self-healing and anomaly detection is the crown feature of Cruise Control, but often the actions taken by Cruise Control provide less visibility to its operation. Adding a slack notification for all the anomaly detection and self-healing will increase Cruise Control visibility.

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
