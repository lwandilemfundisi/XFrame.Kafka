using XFrame.ValueObjects.SingleValueObjects;

namespace XFrame.Kafka;

public class Topic : SingleValueObject<string>
{
    public Topic(string value) : base(value)
    {
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));
    }
}
