namespace XFrame.Kafka;

public sealed record MessageId
{
    public MessageId(string value) => Value = string.IsNullOrWhiteSpace(value) ? throw new ArgumentNullException(nameof(value)) : value;
    public string Value { get; }
    public override string ToString() => Value;
}
