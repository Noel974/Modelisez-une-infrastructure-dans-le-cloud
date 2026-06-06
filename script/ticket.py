import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# Configuration du producteur Redpanda
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

# Listes pour générer des données aléatoires
types_demande = ["Incident", "Demande d'information", "Problème technique", "Maintenance"]
demandes = [
    "Impossible de se connecter",
    "Besoin d'une mise à jour",
    "Erreur sur l'application",
    "Demande de support",
    "Problème de performance"
]
priorites = ["Basse", "Moyenne", "Haute", "Critique"]

def generate_ticket():
    """Génère un ticket conforme au sujet."""
    return {
        "ticket_id": random.randint(1000, 9999),
        "client_id": random.randint(1, 500),  # ID client aléatoire
        "datetime_creation": datetime.now().isoformat(),
        "demande": random.choice(demandes),
        "type_demande": random.choice(types_demande),
        "priorite": random.choice(priorites)
    }

def delivery_report(err, msg):
    """Callback pour confirmer l'envoi."""
    if err is not None:
        print(f"❌ Erreur d'envoi: {err}")
    else:
        print(f"✅ Ticket envoyé dans {msg.topic()}")

print("📤 Envoi de tickets dans Redpanda... (Ctrl+C pour arrêter)")

try:
    while True:
        ticket = generate_ticket()
        producer.produce(
            "client_tickets",
            value=json.dumps(ticket),
            callback=delivery_report
        )
        producer.flush()
        time.sleep(1)  # 1 ticket par seconde
except KeyboardInterrupt:
    print("\n🛑 Producteur arrêté.")
