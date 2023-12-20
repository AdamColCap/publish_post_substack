import os
import pyairtable
from pyairtable import Api
from substack import Api as substack_Api
from substack.post import Post
from time import sleep

def df_to_airtable(df, table):
    df.fillna("No value", inplace=True)

    dico_table = df.to_dict('records')
    table.batch_create(dico_table)

""" Obtenir le tableau de la Airtable avec l'app id et le tblid (dans l'URL, appID/tblID)"""
def get_airtable(app_id, tbl_id):
    api = Api(os.environ['AIRTABLE_TOKEN'])

    return api.table(app_id, tbl_id)

""" Update une case d'une table airtable (l'id commençant par rec) """
def update_airtable_cell(table, rec_id, cell, content):
    table.update(rec_id, {cell: content})

def publish_record_substack(record, api, USER_ID, publish : bool = True, section : str = None):
    titre = record["fields"]["Titre"]
    summary = record["fields"]["summary_text"]
    source = record["fields"]["URL source"]
    share = True if record["fields"]["Notification_mail_substack"] == "True" else False

    print("Posting post :\n"
          f"Publication url = {api.publication_url}\n"
          f"Section = {section}\n"
          f"Title : {titre}\n"
          #f"Body : {summary}\n{source}\n"
          f"publish : {publish}\n"
          f"share : {share}\n"
          )

    post = Post(
        title=titre,
        subtitle="",
        user_id=USER_ID
    )
    post.add({
        "type":"paragraph",
        "content":summary
    })

    post.add({
        'type': 'paragraph',
        'content': [
            {'content': "Lisez l'article original.", 'marks': [
                {'type': "link", 'href': source}
            ]}
        ]
    })

    draft = api.post_draft(post.get_draft())

    if section != None:
        post.set_section("Actualités", api.get_sections())

    api.put_draft(draft.get("id"), draft_section_id=post.draft_section_id)

    api.prepublish_draft(draft.get("id"))
    if publish == True:
        api.publish_draft(draft.get("id"), send = share)

def publish_all_records_substack(api, USER_ID, records, table, publish : bool = True, section : str = None):
    for record in records:
        try:
            publish_record_substack(record, api, USER_ID, publish, section)
            update_airtable_cell(table, record["id"], "Statut_de_publication_substack", "True")
        except Exception as E:
            print(f"Exception : {E}")
            pass
        global DELAY_BETWEEN_API_CALLS
        sleep(DELAY_BETWEEN_API_CALLS)

if __name__ == "__main__":
    """ vars demandées par l'api substack """
    PUB_URL=None#URL DU SUBSTACK exemple : "https://newslokal.substack.com/"
    EMAIL=None #EMAIL DU COMPTE SUBSTACK exemple : "adam@collaborationcapital.tech"
    PASSWORD=None #MOT DE PASSE DU COMPTE SUBSTACK exemple : "Tomate1234"
    USER_ID=None #USER ID LIE AU COMPTE (voir le readme github de l'api pour le récupérer)
    PUBLISH = True #Si True alors on publie, si False alors le poste devient un brouillon
    SECTION = "Actualités" #None pour poster normalement
    DELAY_BETWEEN_API_CALLS = 30 

    """ creation du lien """
    substack_api = substack_Api(
        email=EMAIL,
        password=PASSWORD,
        publication_url=PUB_URL
    )

    """ recuperation des données airtable """
    table = get_airtable("appHREIqHIs32toy0", "tblvM3eejH1sjlraT")
    records = table.all(view="Valid and unpublished Substack")

    """ publication des données en fonction des parametres """
    publish_all_records_substack(substack_api, USER_ID, records, table, PUBLISH, SECTION)
