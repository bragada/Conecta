chave_api = "asfagasg"

from sendgrid = SendGridAPIClient(chave_api)
from sendgrid.helpers.mail import Mail

conta_sendgrid  = SendGridAPIClient(chave_api)

email = Mail(
              from_email = "hkbragada@gmail.com",
              to_emails = "rikibragada@gmail.com",
              subject = "Fala dodóizin" 
              html_content = "<p> ó o cuzin </>
              
