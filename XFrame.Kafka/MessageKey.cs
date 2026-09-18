using XFrame.ValueObjects.SingleValueObjects;

namespace XFrame.Kafka;

public class MessageKey : SingleValueObject<string>
{
    public MessageKey(string value) : base(value)
    {
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));
    }
}
