using XFrame.Ids;
using XFrame.ValueObjects.SingleValueObjects;

namespace XFrame.Kafka;

public class MessageId : SingleValueObject<string>, IIdentity
{
    public MessageId(string value) : base(value)
    {
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));
    }
}
