import { registerJobHandler } from '../job-worker.js';

// Fast-pass (tested end-to-end)
import { handleEcho } from './echo.js';
import { handleHeartbeat } from './heartbeat.js';
import { handleHealthCheck } from './health-check.js';
import { handleSpeakHomepod } from './speak-homepod.js';
import { handleAppleTvControl } from './appletv-control.js';
import { handleSendReminder } from './send-reminder.js';
import { handlePushNotification } from './push-notification.js';
import { handleSendIMessage } from './send-imessage.js';
import { handleGmailSync } from './gmail-sync.js';
import { handleCalendarSync } from './calendar-sync.js';
import { handleInstagramSync } from './instagram-sync.js';
import { handlePhotosSync } from './photos-sync.js';
import { handlePhotoDescribeMcp } from './photo-describe-mcp.js';
import { handleImessageSync } from './imessage-sync.js';
import { handleContactsSync } from './contacts-sync.js';
import { handleReverseGeocode } from './reverse-geocode.js';
import { handleAriaEmailSend } from './aria-email-send.js';
import { handleSendPushover } from './send-pushover.js';
import { handleReviewDigest } from './review-digest.js';

// Infrastructure & monitoring
import { handleExecuteScheduledTask } from './execute-scheduled-task.js';
import { handleDataSourceHealth } from './data-source-health.js';
import { handleAuroraEscalation } from './aurora-escalation.js';
import { handleLogMonitor } from './log-monitor.js';
import { handleHealthCorrelation } from './health-correlation.js';
import { handleSensorDataHygiene } from './sensor-data-hygiene.js';
import { handleContactDedup } from './contact-dedup.js';

// Proactive intelligence pipeline
import { handleContextAccumulate } from './context-accumulate.js';
import { handleSignificanceCheck } from './significance-check.js';
import { handleAnticipationAnalyze } from './anticipation-analyze.js';
import { handleInsightDeliver } from './insight-deliver.js';
import { handlePatternMaintenance } from './pattern-maintenance.js';

// Aurora behavioral intelligence
import { handleAuroraNightly } from './aurora-nightly.js';
import { handleAuroraWeekly } from './aurora-weekly.js';
import { handleAuroraMonthly } from './aurora-monthly.js';

// Cross-source correlation, mood proxy, life transitions, photo social
import { handleCrossCorrelate } from './cross-correlate.js';
import { handleCrossMetricsCompute } from './cross-metrics-compute.js';
import { handleAdvancedCorrelate } from './advanced-correlate.js';
import { handleMoodProxy } from './mood-proxy.js';
import { handleLifeTransition } from './life-transition.js';
import { handlePhotoSocialAnalysis } from './photo-social-analysis.js';

// Social network analysis
import { handleNetworkAnalysis } from './network-analysis.js';
import { handleCommunicationStyle } from './communication-style.js';

// Biographical interview
import { handleBiographicalInterview } from './biographical-interview.js';

// Knowledge graph
import { handleKnowledgeBackfill } from './knowledge-backfill.js';
import { handleKnowledgeSummarize } from './knowledge-summarize.js';
import { handleConversationEmbed } from './conversation-embed.js';

// Memory migration
import { handleMemorySummarize } from './memory-summarize.js';

// Data sync
import { handleDriveSync } from './drive-sync.js';
import { handleDriveIngest } from './drive-ingest.js';

// Reports & digests
import { handleLlmCostReport } from './llm-cost-report.js';
import { handleDigestCompile } from './digest-compile.js';
import { handleDigestEmail } from './digest-email.js';
import { handleDailyBriefing } from './daily-briefing.js';
import { handleCalendarPrep } from './calendar-prep.js';
import { handleMeetingPrepScheduler } from './meeting-prep-scheduler.js';
import { handleCheckIn } from './check-in.js';

// Backfill & batch processing
import { handleGmailBackfill } from './gmail-backfill.js';
import { handleGooglePhotosBackfill } from './google-photos-backfill.js';
import { handleEmbedBackfill } from './embed-backfill.js';
import { handleSentimentBackfill } from './sentiment-backfill.js';
import { handlePhotoDescribe } from './photo-describe.js';
import { handlePhotoHistoryAnalyze } from './photo-history-analyze.js';
import { handleImessageHistoryAnalyze } from './imessage-history-analyze.js';
import { handlePkgBackfill } from './pkg-backfill.js';
import { handleDataImport } from './data-import.js';

// Agent-like jobs (candidates for Layer 2 agent conversion)
import { handleVoiceFollowup } from './voice-followup.js';
import { handleQualityScoring } from './quality-scoring.js';
import { handleJournal } from './journal.js';
import { handleLifeNarration } from './life-narration.js';
import { handleMemoryMaintenance } from './memory-maintenance.js';
import { handleMusicAnalysis } from './music-analysis.js';
import { handleSocialEngagement } from './social-engagement.js';
import { handleLookiRewind } from './looki-rewind.js';
import { handleLookiRealtimePollWithChain as handleLookiRealtimePoll } from './looki-realtime-poll.js';
import { handleLookiDailySync } from './looki-daily-sync.js';

// Data hygiene + merge candidates + voice-print enrichment + person photos
import { handleDataHygiene } from './data-hygiene.js';
import { handleGenerateMergeCandidates } from './generate-merge-candidates.js';
import { handleVoiceprintEnrich } from './voiceprint-enrich.js';
import { handleVoiceAnalysis } from './voice-analysis.js';
import { handlePersonPhotoSync } from './person-photo-sync.js';

// View refresh
import { handleRefreshViews } from './refresh-views.js';

// Apple Notes
import { handleNotesSync } from './notes-sync.js';
import { handleNotesBackfill } from './notes-backfill.js';

// Apple Music
import { handleMusicSync } from './music-sync.js';
import { handleMusicBackfill } from './music-backfill.js';
import { handleAppleMusicSync } from './apple-music-sync.js';

// Gmail transaction extraction
import { handleGmailTransactionExtract } from './gmail-transaction-extract.js';

// People enrichment
import { handlePeopleEnrich } from './people-enrich.js';

// Life chapters
import { handleLifeChapters } from './life-chapters.js';

// Self-improvement
import { handleSelfImproveCode } from './self-improve-code.js';
import { handleSelfImproveDb } from './self-improve-db.js';
import { handleSelfImproveResearch } from './self-improve-research.js';
import { handleSelfImprovementReport } from './self-improvement-report.js';

// Topic modeling
import { handleTopicModel } from './topic-model.js';

export function registerAllHandlers(): void {
  // Delivery channels
  registerJobHandler('echo', handleEcho);
  registerJobHandler('heartbeat', handleHeartbeat);
  registerJobHandler('speak-homepod', handleSpeakHomepod);
  registerJobHandler('appletv-control', handleAppleTvControl);
  registerJobHandler('send-reminder', handleSendReminder);
  registerJobHandler('push-notification', handlePushNotification);
  registerJobHandler('send-imessage', handleSendIMessage);
  registerJobHandler('aria-email-send', handleAriaEmailSend);
  registerJobHandler('send-pushover', handleSendPushover);
  registerJobHandler('review-digest', handleReviewDigest);
  registerJobHandler('gmail-sync', handleGmailSync);
  registerJobHandler('calendar-sync', handleCalendarSync);
  registerJobHandler('instagram-sync', handleInstagramSync);
  registerJobHandler('photos-sync', handlePhotosSync);
  registerJobHandler('photo-describe-mcp', handlePhotoDescribeMcp);
  registerJobHandler('imessage-sync', handleImessageSync);
  registerJobHandler('contacts-sync', handleContactsSync);
  registerJobHandler('reverse-geocode', handleReverseGeocode);

  // Infrastructure & monitoring
  registerJobHandler('execute-scheduled-task', handleExecuteScheduledTask);
  registerJobHandler('health-check', handleHealthCheck);
  registerJobHandler('data-source-health', handleDataSourceHealth);
  registerJobHandler('aurora-escalation', handleAuroraEscalation);
  registerJobHandler('log-monitor', handleLogMonitor);
  registerJobHandler('health-correlation', handleHealthCorrelation);
  registerJobHandler('sensor-data-hygiene', handleSensorDataHygiene);
  registerJobHandler('contact-dedup', handleContactDedup);

  // Proactive intelligence pipeline
  registerJobHandler('context-accumulate', handleContextAccumulate);
  registerJobHandler('significance-check', handleSignificanceCheck);
  registerJobHandler('anticipation-analyze', handleAnticipationAnalyze);
  registerJobHandler('insight-deliver', handleInsightDeliver);
  registerJobHandler('pattern-maintenance', handlePatternMaintenance);

  // Aurora behavioral intelligence
  registerJobHandler('aurora-nightly', handleAuroraNightly);
  registerJobHandler('aurora-weekly', handleAuroraWeekly);
  registerJobHandler('aurora-monthly', handleAuroraMonthly);

  // Knowledge graph
  registerJobHandler('knowledge-backfill', handleKnowledgeBackfill);
  registerJobHandler('knowledge-summarize', handleKnowledgeSummarize);
  registerJobHandler('memory-summarize', handleMemorySummarize);
  registerJobHandler('drive-sync', handleDriveSync);
  registerJobHandler('drive-ingest', handleDriveIngest);
  registerJobHandler('conversation-embed', handleConversationEmbed);

  // Reports & digests
  registerJobHandler('llm-cost-report', handleLlmCostReport);
  registerJobHandler('digest-compile', handleDigestCompile);
  registerJobHandler('digest-email', handleDigestEmail);
  registerJobHandler('daily-briefing', handleDailyBriefing);
  registerJobHandler('calendar-prep', handleCalendarPrep);
  registerJobHandler('meeting-prep-scheduler', handleMeetingPrepScheduler);
  registerJobHandler('check-in', handleCheckIn);

  // Backfill & batch processing
  registerJobHandler('gmail-backfill', handleGmailBackfill);
  registerJobHandler('google-photos-backfill', handleGooglePhotosBackfill);
  registerJobHandler('embed-backfill', handleEmbedBackfill);
  registerJobHandler('sentiment-backfill', handleSentimentBackfill);
  registerJobHandler('photo-describe', handlePhotoDescribe);
  registerJobHandler('photo-history-analyze', handlePhotoHistoryAnalyze);
  registerJobHandler('imessage-history-analyze', handleImessageHistoryAnalyze);
  registerJobHandler('pkg-backfill', handlePkgBackfill);
  registerJobHandler('data-import', handleDataImport);

  // Agent-like jobs
  registerJobHandler('voice-followup', handleVoiceFollowup);
  registerJobHandler('quality-scoring', handleQualityScoring);
  registerJobHandler('journal', handleJournal);
  registerJobHandler('life-narration', handleLifeNarration);
  registerJobHandler('memory-maintenance', handleMemoryMaintenance);
  registerJobHandler('music-analysis', handleMusicAnalysis);
  registerJobHandler('social-engagement', handleSocialEngagement);
  registerJobHandler('looki-rewind', handleLookiRewind);
  registerJobHandler('looki-realtime-poll', handleLookiRealtimePoll);
  registerJobHandler('looki-daily-sync', handleLookiDailySync);

  // Cross-source correlation, mood proxy, life transitions, photo social
  registerJobHandler('cross-correlate', handleCrossCorrelate);
  registerJobHandler('cross-metrics-compute', handleCrossMetricsCompute);
  registerJobHandler('advanced-correlate', handleAdvancedCorrelate);
  registerJobHandler('mood-proxy', handleMoodProxy);
  registerJobHandler('life-transition', handleLifeTransition);
  registerJobHandler('photo-social-analysis', handlePhotoSocialAnalysis);
  registerJobHandler('network-analysis', handleNetworkAnalysis);
  registerJobHandler('communication-style', handleCommunicationStyle);
  registerJobHandler('biographical-interview', handleBiographicalInterview);

  // Data hygiene + merge candidates + voice-print enrichment
  registerJobHandler('data-hygiene', handleDataHygiene);
  registerJobHandler('generate-merge-candidates', handleGenerateMergeCandidates);
  registerJobHandler('voiceprint-enrich', handleVoiceprintEnrich);
  registerJobHandler('voice-analysis', handleVoiceAnalysis);
  registerJobHandler('person-photo-sync', handlePersonPhotoSync);

  // View refresh
  registerJobHandler('refresh-views', handleRefreshViews);

  // Apple Notes
  registerJobHandler('notes-sync', handleNotesSync);
  registerJobHandler('notes-backfill', handleNotesBackfill);

  // Apple Music
  registerJobHandler('music-sync', handleMusicSync);
  registerJobHandler('music-backfill', handleMusicBackfill);
  registerJobHandler('apple-music-sync', handleAppleMusicSync);

  // Gmail transaction extraction
  registerJobHandler('gmail-transaction-extract', handleGmailTransactionExtract);

  // People enrichment
  registerJobHandler('people-enrich', handlePeopleEnrich);

  // Life chapters
  registerJobHandler('life-chapters', handleLifeChapters);

  // Self-improvement
  registerJobHandler('self-improve-code', handleSelfImproveCode);
  registerJobHandler('self-improve-db', handleSelfImproveDb);
  registerJobHandler('self-improve-research', handleSelfImproveResearch);
  registerJobHandler('self-improvement-report', handleSelfImprovementReport);

  // Topic modeling
  registerJobHandler('topic-model', handleTopicModel);
}
