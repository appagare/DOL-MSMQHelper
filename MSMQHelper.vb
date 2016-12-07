'--------------------------------------------------------------------------------------
' Rev #     Heat #          Date          By        Comments
' 1.0.000      
' 1.0.300   HP-3000 Replat  11-1-2005     Mark Lund Added overloaded SendMessage methods to 
'                                                   support recoverable queues. Added an 
'                                                   overloaded ReceiveMessageByLabel without a
'                                                   "retentionPeriod" parameter to allow a message 
'                                                   to be received without removing old messages.
'--------------------------------------------------------------------------------------
Option Strict On
Option Explicit On 

''' <summary>
'''     The MsmqHelper class is a utility class that can be used to perform 
'''     common Message Queuing operations in Microsoft MSMQ.
''' </summary>
''' <remarks>
'''     The MsmqHelper class is a sealed class, and therefore cannot be inherited from. 
'''     It has a private constructor to prevent objects of this type from being created.
''' </remarks>
Public NotInheritable Class MsmqHelper

#Region " Private Methods and Constructors "

    Private Sub New()
        ' Since this class provides only static methods, make the default constructor
        ' private to prevent instances from being created with "New MsmqHelper()".
    End Sub

    Private Shared Function MsgQueueExists(ByVal messageQueuePath As String) As Boolean
        Try
            Dim machineName As String = messageQueuePath.Split(Convert.ToChar("\")).GetValue(0).ToString
            For Each msgQueue As MessageQueue In MessageQueue.GetPrivateQueuesByMachine(machineName)
                If 0 = String.Compare(msgQueue.Path.ToUpper, [String].Format("FormatName:DIRECT=OS:{0}", GetQualifiedName(messageQueuePath)).ToUpper) Then
                    Return True
                End If
            Next
            Return False
        Catch ex As MessageQueueException
            Throw New MsmqHelperException("An error occurred trying to locate the specified queue.", ex)
        End Try
    End Function

    Private Shared Function GetQualifiedName(ByVal messageQueuePath As String) As String
        If messageQueuePath.Split(Convert.ToChar("\")).GetValue(0).ToString = "." Then
            Return messageQueuePath.Remove(0, 1).Insert(0, Environment.MachineName)
        Else
            Return messageQueuePath
        End If
    End Function

    Private Shared Function InitializeMsgQueue(ByVal messageQueuePath As String) As MessageQueue
        Try
            ' If the specified queue does not exist, throw an exception
            If MsgQueueExists(messageQueuePath) Then
                Return New MessageQueue([String].Format("FormatName:DIRECT=OS:{0}", GetQualifiedName(messageQueuePath)))
            Else
                Throw New MsmqHelperException("The specified queue does not exist.")
            End If
        Catch ex As MessageQueueException
            Throw New MsmqHelperException("A message queue initialization error has occurred.", ex)
        End Try
    End Function

#End Region

#Region " Shared Utility Methods "
    ''' <summary>
    '''     Sends a message to the queue.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageBody">A string that specifies the message contents.</param>
    ''' <remarks>
    '''     The size of the messageBody parameter cannot exceed 4 MB.
    ''' </remarks>
    Public Overloads Shared Sub SendMessage(ByVal messageQueuePath As String, _
        ByVal messageBody As String)
        ' call the common method, passing the default .Net False value for the Recoverable property

        SendMessage(messageQueuePath, messageBody, False)

    End Sub
    ''' <summary>
    '''     Sends a message to the queue.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageBody">A string that specifies the message contents.</param>
    ''' <param name="Recoverable">Boolean value to support recoverable queues.</param>
    ''' <remarks>
    '''     The size of the messageBody parameter cannot exceed 4 MB.
    ''' </remarks>
    Public Overloads Shared Sub SendMessage(ByVal messageQueuePath As String, _
        ByVal messageBody As String, _
        ByVal Recoverable As Boolean)
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)

        ' Create a new message.
        Dim msg As New Message

        ' Set the message properties.
        msg.Body = messageBody
        msg.Recoverable = Recoverable 'added 10/17/2005 MTL

        ' Send the message to the queue.
        msgQueue.Send(msg)

        ' Close the queue and free resources.
        msgQueue.Close()
    End Sub

    ''' <summary>
    '''     Sends a message to the queue with a user-defined label.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageBody">A string that specifies the message contents.</param>
    ''' <param name="messageLabel">A string that specifies the message label</param>
    ''' <remarks>
    '''     The size of the messageBody parameter cannot exceed 4 MB. The message label is
    '''     not a unique identifier in MSMQ. If unique labels are required they must be managed
    '''     by the calling application.
    ''' </remarks>
    Public Overloads Shared Sub SendMessage(ByVal messageQueuePath As String, ByVal messageBody As String, ByVal messageLabel As String)
        ' call the common method, passing the default .Net False value for the Recoverable property
        SendMessage(messageQueuePath, messageBody, messageLabel, False)
    End Sub

    ''' <summary>
    '''     Sends a message to the queue with a user-defined label.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageBody">A string that specifies the message contents.</param>
    ''' <param name="messageLabel">A string that specifies the message label</param>
    ''' <param name="Recoverable">Boolean value to support recoverable queues.</param>
    ''' <remarks>
    '''     The size of the messageBody parameter cannot exceed 4 MB. The message label is
    '''     not a unique identifier in MSMQ. If unique labels are required they must be managed
    '''     by the calling application.
    ''' </remarks>
    Public Overloads Shared Sub SendMessage(ByVal messageQueuePath As String, _
        ByVal messageBody As String, _
        ByVal messageLabel As String, _
        ByVal Recoverable As Boolean)
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)

        ' Create a new message.
        Dim msg As New Message

        ' Set the message properties.
        msg.Body = messageBody
        msg.Label = messageLabel
        msg.Recoverable = Recoverable

        ' Send the message to the queue.
        msgQueue.Send(msg)

        ' Close the queue and free resources.
        msgQueue.Close()
    End Sub

    ''' <summary>
    '''     Receives the first message in the queue, without removing it from the queue.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="timeout">A <see cref="System.TimeSpan"/> that indicates the time to wait until a new message is available for inspection.</param>
    ''' <returns>The contents of the selected message.  If no message is received within specified timeout period return Nothing.</returns>
    Public Shared Function PeekMessage(ByVal messageQueuePath As String, ByVal timeout As TimeSpan) As String
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)

        ' Set the formatter to indicate body contains a string.
        msgQueue.Formatter = New XmlMessageFormatter(New Type() {GetType(String)})

        ' Recieve a message
        Try
            Dim msg As Message

            If msgQueue.CanRead Then
                msg = msgQueue.Peek(timeout)
            Else
                Throw New MsmqHelperException("An error occurred trying to peek from the queue.")
            End If

            Return msg.Body.ToString()
        Catch ex As MessageQueueException
            ' Handle no message arriving in the queue.
            If ex.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                Return Nothing
            Else
                Throw New MsmqHelperException("An error occurred trying to peek the message.", ex)
            End If
        Finally
            ' Close the queue and free resources.
            msgQueue.Close()
        End Try

    End Function

    ''' <summary>
    '''     Receives the first message in the queue, removing it from the queue.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="timeout">A <see cref="System.TimeSpan"/> that indicates the time to wait until a new message is available for inspection.</param>
    ''' <returns>The contents of the selected message.  If no message is received within specified timeout period return Nothing.</returns>
    Public Shared Function ReceiveMessage(ByVal messageQueuePath As String, ByVal timeout As TimeSpan) As String
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)

        ' Set the formatter to indicate body contains a string.
        msgQueue.Formatter = New XmlMessageFormatter(New Type() {GetType(String)})

        ' Recieve a message
        Try
            Dim msg As Message

            If msgQueue.CanRead Then
                msg = msgQueue.Receive(timeout)
            Else
                Throw New MsmqHelperException("An error occurred trying to read from the queue.")
            End If

            Return msg.Body.ToString()
        Catch ex As MessageQueueException
            ' Handle no message arriving in the queue.
            If ex.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                Return Nothing
            Else
                Throw New MsmqHelperException("An error occurred trying to receive the message.", ex)
            End If
        Finally
            ' Close the queue and free resources.
            msgQueue.Close()
        End Try

    End Function

    ''' <summary>
    '''     Receives the message that matches the given identifier, removing it from the queue.
    '''     Also removes any messages from the queue that are past the retention period (retentionPeriod) parameter.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageLabel">A string that specifies the message label</param>
    ''' <param name="retentionPeriod">A TimeSpan that is used to test whether messages in the queue should be removed.</param>
    ''' <returns>The contents of the selected message. If no matching message is found it returns Nothing.</returns>
    Public Overloads Shared Function ReceiveMessageByLabel(ByVal messageQueuePath As String, ByVal messageLabel As String, ByVal retentionPeriod As TimeSpan) As String
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)
        Dim msgBody As String = Nothing

        ' Set the formatter to indicate body contains a string.
        msgQueue.Formatter = New XmlMessageFormatter(New Type() {GetType(String)})

        ' Get a cursor into the messages in the queue.
        Dim msgEnumerator As MessageEnumerator = msgQueue.GetMessageEnumerator()

        ' Specify that the messages's label should be read.
        msgQueue.MessageReadPropertyFilter.Label = True

        ' Specify that the messages's arrived time should be read.
        msgQueue.MessageReadPropertyFilter.ArrivedTime = True

        ' Move to the next message and examine its label.
        While msgEnumerator.MoveNext()
            If msgEnumerator.Current.Label.TrimEnd = messageLabel.TrimEnd Then
                ' Receive the current message
                Dim msg As Message

                If msgQueue.CanRead Then
                    msg = msgQueue.ReceiveById(msgEnumerator.Current.Id)
                    msgBody = msg.Body.ToString()
                Else
                    Throw New MsmqHelperException("An error occurred trying to read from the queue.")
                End If

            ElseIf DateDiff(DateInterval.Second, msgEnumerator.Current.ArrivedTime, Now) > retentionPeriod.TotalSeconds Then
                ' Remove old message from the queue
                If msgQueue.CanRead Then
                    msgQueue.ReceiveById(msgEnumerator.Current.Id)
                Else
                    Throw New MsmqHelperException("An error occurred trying to read from the queue.")
                End If
            End If
        End While

        ' Close the queue and free resources.
        msgQueue.Close()

        Return msgBody
    End Function

    ''' <summary>
    '''     Receives the message that matches the given identifier, removing it from the queue.
    ''' </summary>
    ''' <param name="messageQueuePath">A valid message queue path.</param>
    ''' <param name="messageLabel">A string that specifies the message label</param>
    ''' <returns>The contents of the selected message. If no matching message is found it returns Nothing.</returns>
    Public Overloads Shared Function ReceiveMessageByLabel(ByVal messageQueuePath As String, ByVal messageLabel As String) As String
        ' Initialize message queue
        Dim msgQueue As MessageQueue = InitializeMsgQueue(messageQueuePath)
        Dim msgBody As String = Nothing

        ' Set the formatter to indicate body contains a string.
        msgQueue.Formatter = New XmlMessageFormatter(New Type() {GetType(String)})

        ' Get a cursor into the messages in the queue.
        Dim msgEnumerator As MessageEnumerator = msgQueue.GetMessageEnumerator()

        ' Specify that the messages's label should be read.
        msgQueue.MessageReadPropertyFilter.Label = True

        ' Specify that the messages's arrived time should be read.
        msgQueue.MessageReadPropertyFilter.ArrivedTime = True

        ' Move to the next message and examine its label.
        While msgEnumerator.MoveNext()
            If msgEnumerator.Current.Label.TrimEnd = messageLabel.TrimEnd Then
                ' Receive the current message
                Dim msg As Message

                If msgQueue.CanRead Then
                    msg = msgQueue.ReceiveById(msgEnumerator.Current.Id)
                    msgBody = msg.Body.ToString()
                Else
                    Throw New MsmqHelperException("An error occurred trying to read from the queue.")
                End If
            End If
        End While

        ' Close the queue and free resources.
        msgQueue.Close()

        Return msgBody
    End Function

#End Region

End Class



