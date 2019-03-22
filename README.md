# Tableau migration

## installation :

```
pip install -r requirements
```

## execution
Just configure a file in `params_local.yml`
```
servers:
  tableau1:                        # <= a tableau server
    url: "http://tableau.in"       # <= it's url
    site_id: site_in               # <= site where you want your action (delete, crete, migrate ...)
    user: Admin_user_in            # <= tableau super user
    password: Admin_password       # <= and password
  tableau2:
    url: "http://tableau.out"
    db:                            # db is necessary for target tableau server
      ip: 10.0.15.20               # its ip
      passwords:                   # and connections user/password 
        db_user: db_user_password
        other_user: other_password
    site_id: site_out
    site_name: 'My wonderful site' # for creation, the title of the site must be done
    user: Admin_user_out
    password: Admin_password
run:
  config:      
    servers:
      in: tableau1                 # configure input and output
      out: tableau2
  actions:                         # this sequence can be changed
    - delete_site                  # delete acts on output (if exists)
    - create_site                  # create site on output
    - migrate_projects             # copy projects from input to output, copying hierarchy
    - migrate_datasources          # copy datasources, migrating site db and crendials
    - migrate_workbooks            # copy workbooks
```
