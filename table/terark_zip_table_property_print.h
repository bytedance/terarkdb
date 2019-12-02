  const double GiB = 1ull << 30;

  M_String(localTempDir);
  M_String(indexType);
  M_NumFmt(checksumLevel            , "%d");
  M_NumFmt(checksumSmallValSize     , "%d");
  M_NumFmt(entropyAlgo              , "%d");
  M_NumFmt(indexNestLevel           , "%d");
  M_NumFmt(indexNestScale           , "%d");
  M_NumFmt(indexTempLevel           , "%d");
  M_NumFmt(terarkZipMinLevel        , "%d");
  M_NumFmt(minDictZipValueSize      , "%u");
  M_NumFmt(keyPrefixLen             , "%u");
  M_NumFmt(debugLevel               , "%d");
  M_Boolea(enableCompressionProbe);
  M_Boolea(useSuffixArrayLocalMatch);
  M_Boolea(warmUpIndexOnOpen);
  M_Boolea(warmUpValueOnOpen);
  M_Boolea(disableSecondPassIter);
  M_Boolea(disableCompressDict);
  M_Boolea(optimizeCpuL3Cache);
  M_Boolea(forceMetaInMemory);
  M_Boolea(enableEntropyStore);
  M_NumFmt(minPreadLen              , "%d");
  M_NumFmt(offsetArrayBlockUnits    , "%d");
  M_NumFmt(sampleRatio              , "%f");
  M_NumFmt(indexCacheRatio          , "%f");
  M_NumGiB(softZipWorkingMemLimit);
  M_NumGiB(hardZipWorkingMemLimit);
  M_NumGiB(smallTaskMemory);
  M_NumGiB(singleIndexMinSize);
  M_NumGiB(singleIndexMaxSize);
  M_NumGiB(cacheCapacityBytes);
  M_NumFmt(cacheShards              , "%d");

#undef M_NumFmt
#undef M_NumGiB
#undef M_Boolea
#undef M_String
