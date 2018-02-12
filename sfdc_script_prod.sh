#! /bin/bash 
java -Dhttps.protocols=TLSv1.1,TLSv1.2 -cp ".:*" PartnerWSDL && java -Dhttps.protocols=TLSv1.1,TLSv1.2 -cp ".:*" FullLoadProd
