# Unread message count indicator
Similar to how the WhatsApp icon shows a count of unique unread chat windows above its icon, we have to implement an efficient logic to render that number.

<img src="https://media.idownloadblog.com/wp-content/uploads/2025/02/WhatsApp-icon-with-red-unread-message-badge.jpg"/>


## The 2 approaches
1. **On the fly computation**

   As soon as a new message, arrives we have to update the unread message count for the given user.

3. **Lazy computation**

   As the name implies, we can "lazily" compute the unread message count for the given user.

## Upcoming...
1. Load testing framework using artillery.
