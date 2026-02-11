namespace Signals.Common.Utilities;
public static class DateTimeOffsetExtensions
{
    public static ulong ToUnixTimeNanoseconds(this DateTimeOffset dateTime) => (ulong)dateTime.ToUnixTimeMilliseconds() * 1_000_000UL;
}