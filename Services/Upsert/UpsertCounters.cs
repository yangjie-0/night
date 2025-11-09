namespace ProductDataIngestion.Services.Upsert
{
    /// <summary>
    /// UPSERT段階の件数カウンタを管理するヘルパ。
    /// </summary>
    public sealed class UpsertCounters
    {
        private int _read;
        private int _insert;
        private int _update;
        private int _skip;
        private int _error;

        public int Read => _read;
        public int Insert => _insert;
        public int Update => _update;
        public int Skip => _skip;
        public int Error => _error;

        public void Reset()
        {
            _read = 0;
            _insert = 0;
            _update = 0;
            _skip = 0;
            _error = 0;
        }

        public void IncrementRead() => _read++;
        public void IncrementInsert() => _insert++;
        public void IncrementUpdate() => _update++;
        public void IncrementSkip() => _skip++;
        public void IncrementError() => _error++;

        public UpsertCounters Clone()
        {
            return new UpsertCounters
            {
                _read = _read,
                _insert = _insert,
                _update = _update,
                _skip = _skip,
                _error = _error
            };
        }
    }
}
