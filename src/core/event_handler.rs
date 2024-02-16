use super::{Event, Message};

/// the entry-point for all actions in demon
pub async fn handle(event: Event) {
    match event {
        Event::Internal(m) => {
            match m {
                Message::Membership => todo!(),
                Message::Sequencer => todo!(),
                Message::Gossip => todo!(),
            }
        },
        Event::Client(r) => todo!(),
    }
}
