# Synpopsis
This projects is part of my volunteer work for a german NGO.
The aim is to take csv.exports from FundraisingBox and Twingle and import them automatically into mailchimp, but only take those who want a newsletter.

# ETL Steps
Data is read and the needed fields are extracted. Afterwards all entries get aggregated on the mail adress (used as PK) and send to mailchimp via their API.


