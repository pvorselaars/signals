namespace Signals.Common.Utilities;
public static class DateTimeExtensions
{
    public static ulong ToUnixTimeNanoseconds(this DateTime dateTime) => (ulong)(dateTime.Ticks - DateTime.UnixEpoch.Ticks) * 100;
}