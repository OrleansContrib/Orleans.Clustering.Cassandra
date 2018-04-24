namespace NuClear.Broadway.Worker
{
    public struct CommandLine
    {
        public const char ArgumentKeySeparator = '=';
        public const char ArgumentValueSeparator = ',';
        public const string HelpOptionTemplate = "-h|--help";

        public struct Commands
        {
            public const string Import = "import";
        }
        
        public struct CommandTypes
        {
            public const string Firms = "firms";
            public const string FlowKaleidoscope = "flow-kaleidoscope";
        }

        public struct Arguments
        {
        }
    }
}