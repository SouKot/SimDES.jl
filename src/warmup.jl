# warmup.jl — WelchDetector re-exported from SimCore (Sprint 4I)
#
# WelchDetector was moved from SimDES → SimCore so that StatsPipeline
# (which lives in SimCore) can use it without a circular dependency.
#
# All existing SimDES callers continue to work unchanged via this re-export.
using SimCore: WelchDetector, update!, warmup_complete
