import { Message, Stan } from 'node-nats-streaming'

import { Event } from './interfaces'

export abstract class Listener<T extends Event> {
  // name of channel listener is going to listen to
  abstract subject: T['subject']

  // name of queue group listener will join
  abstract queueGroupName: string

  // // cb to run when message is received
  abstract onMessage(data: T['data'], msg: Message): void

  // number of seconds listener has to ack a message
  protected ackwait = 5000 // 5s

  constructor(private client: Stan) {}

  subscriptionOptions() {
    return this.client
      .subscriptionOptions()
      .setDeliverAllAvailable()
      .setManualAckMode(true)
      .setAckWait(this.ackwait)
      .setDurableName(this.queueGroupName)
  }

  // code to set up listener
  listen(): void {
    const subscription = this.client.subscribe(
      this.subject,
      this.queueGroupName,
      this.subscriptionOptions()
    )

    subscription.on('message', (msg: Message) => {
      console.log(`Message received: ${this.subject} / ${this.queueGroupName}`)

      const parsedData = this.parseMessage(msg)
      this.onMessage(parsedData, msg)
    })
  }

  // helper function
  parseMessage(msg: Message): any {
    const data = msg.getData()
    return typeof data === 'string'
      ? JSON.parse(data) // string
      : JSON.parse(data.toString('utf8')) // Buffer
  }
}
