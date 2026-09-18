namespace XFrame.Kafka.Messages;

public sealed class KafkaMessage
{
    public KafkaMessage(
        string message, 
        IReadOnlyDictionary<string, string> headers, 
        Topic topic, 
        MessageKey key, 
        MessageId messageId)
    {
        Message = string.IsNullOrWhiteSpace(message) ? throw new ArgumentNullException(nameof(message)) : message;
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Key = key ?? throw new ArgumentNullException(nameof(key));
        MessageId = messageId ?? throw new ArgumentNullException(nameof(messageId));
    }

    public string Message { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }
    public Topic Topic { get; }
    public MessageKey Key { get; }
    public MessageId MessageId { get; }
    public override string ToString() => $"{{Topic: {Topic}, Key: {Key}, MessageId: {MessageId}, Headers: {Headers.Count}, Bytes: {System.Text.Encoding.UTF8.GetByteCount(Message)}}}";
}
