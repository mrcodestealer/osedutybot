import numpy as np
import torch
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
from transformers import Trainer, TrainingArguments
from datasets import Dataset
import nltk
import re
import pickle
import random
from nltk.stem import PorterStemmer
from sklearn.model_selection import train_test_split
from thefuzz import fuzz, process

# Download NLTK data (only once)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)

stemmer = PorterStemmer()

# ------------------------------------------------------------
# 1. PARSE EMPLOYEE DATA (fixed version)
# ------------------------------------------------------------
def parse_employee_data(fpms_text, db_text, platformSRE_text, cpms_text, pms_text, fe_text):
    employees = {}
    
    # FPMS block: uses .CONTAIN
    fpms_pattern = r'\[Duty name\]\.CONTAIN\("([^"]+)"\)\s*,\s*"(\d+)"'
    for match in re.finditer(fpms_pattern, fpms_text):
        full_name, phone = match.groups()
        base_name = re.sub(r'\s*\([^)]*\)', '', full_name).strip()
        employees[base_name] = {
            'phone': phone,
            'department': 'FPMS',
            'full_name': full_name
        }
    
    # DB block: uses REGEXMATCH (without LOWER)
    db_pattern = r'REGEXMATCH\(\[Duty name\],\s*"([^"]+)"\)\s*,\s*"(\d+)"'
    for match in re.finditer(db_pattern, db_text):
        name, phone = match.groups()
        employees[name] = {
            'phone': phone,
            'department': 'DB',
            'full_name': name
        }
    
    regex_general = r'REGEXMATCH(?:\(LOWER\(\[Duty name\]\)|\(\[Duty name\]\)),\s*"([^"]+)"\)\s*,\s*"(\d+)"'
    
    # PlatformSRE block
    for match in re.finditer(regex_general, platformSRE_text):
        name, phone = match.groups()
        employees[name] = {
            'phone': phone,
            'department': 'PlatformSRE',
            'full_name': name
        }
    
    # CPMS block
    for match in re.finditer(regex_general, cpms_text):
        name, phone = match.groups()
        employees[name] = {
            'phone': phone,
            'department': 'CPMS',
            'full_name': name
        }
    
    # PMS block
    for match in re.finditer(regex_general, pms_text):
        name, phone = match.groups()
        employees[name] = {
            'phone': phone,
            'department': 'PMS',
            'full_name': name
        }
    
    # FE block
    for match in re.finditer(regex_general, fe_text):
        name, phone = match.groups()
        employees[name] = {
            'phone': phone,
            'department': 'FE',
            'full_name': name
        }
    
    return employees

def generate_person_intents(employees):
    person_intents = []
    for base_name, info in employees.items():
        full_name = info['full_name']
        phone = info['phone']
        dept = info['department']

        patterns = [
            f"what is {base_name}'s phone number",
            f"what is {base_name}'s number",
            f"what is the phone number for {base_name}",
            f"who is {base_name}",
            f"tell me about {base_name}",
            f"what department is {base_name} in",
            f"where does {base_name} work",
            f"what is {full_name}'s phone number",
            f"what is {full_name}'s number",
            f"who is {full_name}",
            f"tell me about {full_name}",
            f"what department is {full_name} in",
            f"where does {full_name} work",
            f"i need {base_name}'s contact",
            f"can i have {base_name}'s phone number",
            f"how can i reach {base_name}",
            base_name,
            base_name.lower(),
            f"what is {base_name}",
            f"who is {base_name}",
            f"{base_name} phone",
            f"{base_name} number",
            f"{base_name} department",
        ]

        if ' ' in base_name:
            parts = base_name.split()
            concatenated = base_name.replace(' ', '')
            patterns.append(concatenated)
            patterns.append(concatenated.lower())
            for part in parts:
                patterns.extend([part, f"what is {part}", f"who is {part}", f"{part} phone"])
                if len(part) > 3:
                    abbrev = part[:3]
                    patterns.extend([abbrev, f"what is {abbrev}", f"who is {abbrev}", f"{abbrev} phone"])
        else:
            if len(base_name) > 3:
                abbrev = base_name[:3]
                patterns.extend([abbrev, f"what is {abbrev}", f"who is {abbrev}", f"{abbrev} phone"])

        patterns = list(dict.fromkeys(patterns))

        responses = [
            f"{base_name} is in the {dept} department. Their phone number is {phone}.",
            f"{base_name} works in {dept}. You can call them at {phone}.",
            f"The number for {base_name} ({dept}) is {phone}.",
        ]

        tag = f"person_{base_name.lower().replace(' ', '_')}"
        person_intents.append({
            "tag": tag,
            "patterns": patterns,
            "responses": responses
        })
    return person_intents

# ------------------------------------------------------------
# 2. PYTORCH TRAINER
# ------------------------------------------------------------
class PyTorchTrainer:
    def __init__(self, model_name='distilbert-base-uncased'):
        self.tokenizer = DistilBertTokenizer.from_pretrained(model_name)
        self.model_name = model_name

    def prepare_training_data(self, intents):
        texts = []
        labels = []
        tag_to_id = {}
        for idx, intent in enumerate(intents['intents']):
            tag = intent['tag']
            tag_to_id[tag] = idx
            for pattern in intent['patterns']:
                texts.append(pattern)
                labels.append(idx)
        data = {'text': texts, 'label': labels}
        dataset = Dataset.from_dict(data)
        return dataset, tag_to_id

    def tokenize_function(self, examples):
        return self.tokenizer(examples['text'], truncation=True, padding='max_length', max_length=32)

    def train(self, intents, output_dir='deep_person_chatbot_pt'):
        dataset, tag_to_id = self.prepare_training_data(intents)
        dataset = dataset.train_test_split(test_size=0.2, seed=42)
        train_dataset = dataset['train'].map(self.tokenize_function, batched=True)
        val_dataset = dataset['test'].map(self.tokenize_function, batched=True)

        # Set format for PyTorch
        train_dataset.set_format(type='torch', columns=['input_ids', 'attention_mask', 'label'])
        val_dataset.set_format(type='torch', columns=['input_ids', 'attention_mask', 'label'])

        model = DistilBertForSequenceClassification.from_pretrained(
            self.model_name, num_labels=len(tag_to_id)
        )

        training_args = TrainingArguments(
            output_dir=output_dir,
            eval_strategy="epoch",
            save_strategy="epoch",
            learning_rate=2e-5,
            per_device_train_batch_size=16,
            per_device_eval_batch_size=16,
            num_train_epochs=10,
            weight_decay=0.01,
            load_best_model_at_end=True,
            metric_for_best_model='accuracy',
            push_to_hub=False,
        )

        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=val_dataset,
            # tokenizer=self.tokenizer,   # <-- REMOVED this line
            compute_metrics=lambda p: {'accuracy': (p.predictions.argmax(-1) == p.label_ids).mean()}
        )

        trainer.train()
        trainer.save_model(output_dir)
        self.tokenizer.save_pretrained(output_dir)

        # Save metadata
        with open(f'{output_dir}/metadata.pkl', 'wb') as f:
            pickle.dump({
                'tag_to_id': tag_to_id,
                'id_to_tag': {v: k for k, v in tag_to_id.items()},
                'intents': intents
            }, f)

        print(f"\n✅ PyTorch model saved to {output_dir}")
        return model, tag_to_id

# ------------------------------------------------------------
# 3. PYTORCH CHATBOT CLASS (with fuzzy matching fallback)
# ------------------------------------------------------------
class PyTorchChatBot:
    def __init__(self, model_dir):
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.tokenizer = DistilBertTokenizer.from_pretrained(model_dir)
        self.model = DistilBertForSequenceClassification.from_pretrained(model_dir).to(self.device)
        self.model.eval()

        with open(f'{model_dir}/metadata.pkl', 'rb') as f:
            meta = pickle.load(f)
            self.tag_to_id = meta['tag_to_id']
            self.id_to_tag = meta['id_to_tag']
            self.intents = meta['intents']

        # Build fuzzy matching dictionary: all possible name forms -> (base_name, phone, dept)
        self.name_variants = {}
        for intent in self.intents['intents']:
            if intent['tag'].startswith('person_'):
                base_name = intent['tag'][7:].replace('_', ' ').title()
                sample_response = intent['responses'][0]
                phone_match = re.search(r'\d{10,}', sample_response)
                phone = phone_match.group() if phone_match else "unknown"
                dept = "unknown"
                for d in ['FPMS', 'DB', 'PlatformSRE', 'CPMS', 'PMS', 'FE']:
                    if d in sample_response:
                        dept = d
                        break
                self.name_variants[base_name] = (base_name, phone, dept)
                for part in base_name.split():
                    if len(part) > 1:
                        self.name_variants[part] = (base_name, phone, dept)
                self.name_variants[base_name.lower()] = (base_name, phone, dept)
                for part in base_name.split():
                    self.name_variants[part.lower()] = (base_name, phone, dept)
                concatenated = base_name.replace(' ', '')
                self.name_variants[concatenated] = (base_name, phone, dept)
                self.name_variants[concatenated.lower()] = (base_name, phone, dept)

        self.conversation_history = []

    def predict_intent(self, text):
        inputs = self.tokenizer(text, return_tensors='pt', truncation=True, padding='max_length', max_length=32)
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        with torch.no_grad():
            outputs = self.model(**inputs)
        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1).cpu().numpy()[0]
        predicted_id = np.argmax(probabilities)
        confidence = probabilities[predicted_id]
        tag = self.id_to_tag[predicted_id]
        return tag, confidence

    def get_response(self, tag):
        for intent in self.intents['intents']:
            if intent['tag'] == tag:
                return random.choice(intent['responses'])
        return "I'm not sure how to respond to that."

    def fuzzy_match_person(self, text):
        """Try to find a person name in the input using fuzzy matching."""
        words = text.split()
        candidates = []

        # Check each word against name variants
        for word in words:
            if len(word) < 3:
                continue
            best = process.extractOne(word, self.name_variants.keys(), scorer=fuzz.partial_ratio)
            if best and best[1] > 65:
                candidates.append((best[0], best[1]))

        # Also try the whole input as a single phrase
        best_phrase = process.extractOne(text, self.name_variants.keys(), scorer=fuzz.partial_ratio)
        if best_phrase and best_phrase[1] > 65:
            candidates.append((best_phrase[0], best_phrase[1]))

        if not candidates:
            return None

        best_match = max(candidates, key=lambda x: x[1])
        matched_variant = best_match[0]
        base_name, phone, dept = self.name_variants[matched_variant]
        return base_name, phone, dept

    def chat(self):
        print("🤖 PyTorch Deep Learning ChatBot (DistilBERT) is ready! Type 'quit' to exit.")
        print("💡 You can ask about people's phone numbers and departments (typos now handled!).")
        print("-" * 50)

        while True:
            user_input = input("\nYou: ").strip()
            if user_input.lower() in ['quit', 'exit', 'bye']:
                print("Bot: Goodbye! Thanks for chatting! 👋")
                break
            if not user_input:
                continue

            self.conversation_history.append(user_input)
            intent, confidence = self.predict_intent(user_input)

            if confidence > 0.7:
                response = self.get_response(intent)
                print(f"Bot: {response}")
                print(f"   (Detected: '{intent}' - {confidence:.2f} confidence)")
            else:
                match = self.fuzzy_match_person(user_input)
                if match:
                    base_name, phone, dept = match
                    print(f"Bot: {base_name} is in the {dept} department. Their phone number is {phone}.")
                    print("   (Found via fuzzy matching)")
                else:
                    print("Bot: I'm not sure I understand. Could you rephrase that?")
                    print(f"   (Best guess: '{intent}' - {confidence:.2f} confidence)")

# ------------------------------------------------------------
# 4. MAIN EXECUTION (Training + Chat)
# ------------------------------------------------------------
if __name__ == "__main__":
    # Data blocks (your full FPMS and DB data)
    FPMS_DATA = """
    IFS(
      [Duty name].CONTAIN("David(Final level)"),
      "60102703549",
      [Duty name].CONTAIN("Olivia(Final Level)"),
      "60163661007",
      [Duty name].CONTAIN("Eason (Manager)"),
      "60129687432",
      [Duty name].CONTAIN("Yang (Team Lead)"),
      "60168109045",
      [Duty name].CONTAIN("Bk (Team Lead)"),
      "601133798396",
      [Duty name].CONTAIN("Teck Loong"),
      "60173549088",
      [Duty name].CONTAIN("Kai Hao"),
      "601127853910",
      [Duty name].CONTAIN("Henry"),
      "60194133366",
      [Duty name].CONTAIN("Guan Zhong"),
      "60185741893",
      [Duty name].CONTAIN("Chun Khai"),
      "60174463679",
      [Duty name].CONTAIN("Ryan"),
      "60105925678",
      [Duty name].CONTAIN("Fook Cheng"),
      "601136355366",
      [Duty name].CONTAIN("Eric Lee"),
      "60164091004",
      [Duty name].CONTAIN("Nicole Lai"),
      "60126658368",
      [Duty name].CONTAIN("Louis"),
      "60168693549",
      [Duty name].CONTAIN("Jun Xian"),
      "60149815562",
      [Duty name].CONTAIN("Li Yi"),
      "60125288264",
      [Duty name].CONTAIN("Tiger"),
      "60129333549",
      [Duty name].CONTAIN("Chun Xian"),
      "60125142293",
      [Duty name].CONTAIN("See Hong"),
      "601110613601",
      [Duty name].CONTAIN("Nicson"),
      "60176817033",
      [Duty name].CONTAIN("Harvey"),
      "60165841983",
      [Duty name].CONTAIN("Lee Juan"),
      "60172418903",
      [Duty name].CONTAIN("Jie Qi"),
      "601136116252",
      [Duty name].CONTAIN("Edmond"),
      "601131530131",
      [Duty name].CONTAIN("Shi Eng"),
      "60142667241"
    )
    """

    DB_DATA = """
    IFS(
      REGEXMATCH([Duty name], "Monlong"),
      "60104237748",
      REGEXMATCH([Duty name], "Ziyang"),
      "60102398909",
      REGEXMATCH([Duty name], "Ken"),
      "60192336398",
      REGEXMATCH([Duty name], "Kah Zheng"),
      "60169294328",
      REGEXMATCH([Duty name], "Jien yang"),
      "60165168363"
    )
    """
    
    platformSRE_DATA = """
    IFS(
        REGEXMATCH([Duty name], "Adrian"),
        "60123156848",
        REGEXMATCH([Duty name], "Yoon Hong"),
        "60162701926",
        REGEXMATCH([Duty name], "Alex Tai"),
        "60146120200",
        REGEXMATCH([Duty name], "Kelvin"),
        "60125989338",
        REGEXMATCH([Duty name], "Wei Siong"),
        "60163132882",
        REGEXMATCH([Duty name], "Bo Wei"),
        "60173612963",
        REGEXMATCH([Duty name], "Jay"),
        "601116392152",
        REGEXMATCH([Duty name], "Linus Lim"),
        "60187885330",
        REGEXMATCH([Duty name], "Jeng Liang"),
        "60182923531",
        REGEXMATCH([Duty name], "Misa"),
        "60125020451",
        REGEXMATCH([Duty name], "Kai Xuan"),
        "60107809495"
    )
    """
    
    cpms_DATA = """
    IFS(
    REGEXMATCH([Duty name], "Kh"),
    "60129288595",
    REGEXMATCH([Duty name], "Kingsley"),
    "60182721129",
    REGEXMATCH([Duty name], "Koo"),
    "60126446277",
    REGEXMATCH([Duty name], "Raymond"),
    "60149329850",
    REGEXMATCH([Duty name], "Sandy"),
    "60189634165",
    REGEXMATCH([Duty name], "Square"),
    "60172513573",
    REGEXMATCH([Duty name], "Wailoon"),
    "60182247838",
    REGEXMATCH([Duty name], "Wei Tung"),
    "60128072398",
    REGEXMATCH([Duty name], "YC"),
    "60172112912",
    REGEXMATCH([Duty name], "Alex EHZ"),
    "60164401582",
    REGEXMATCH([Duty name], "Liew Yih Chan"),
    "60172112912",
    REGEXMATCH([Duty name], "Jason"),
    "0122537316"
    )
    """
    
    pms_DATA = """
    IFS(
    REGEXMATCH([Duty name], "Darren"),
    "60184608230",

    REGEXMATCH([Duty name], "Larrie Adriano"),
    "639053043671",

    REGEXMATCH([Duty name], "Manuel Lorenzo Pereira"),
    "639083020911",

    REGEXMATCH([Duty name], "Bien Dave Magno"),
    "639082933148",

    REGEXMATCH([Duty name], "Alvis"),
    "60173232967",

    REGEXMATCH([Duty name], "Ramel John Brillantes"),
    "639223457482",

    REGEXMATCH([Duty name], "Ray Ranil Quijada"),
    "639671133068",

    REGEXMATCH([Duty name], "Lucrisha Mae Siquijor"),
    "639232774225",

    REGEXMATCH([Duty name], "Vince Harresh Rodriguez"),
    "639761473998",

    REGEXMATCH([Duty name], "Ronjon Nuno"),
    "639613769687"
    )
    """
    
    fe_DATA = """
    IFS(
        REGEXMATCH(LOWER([Duty name]), "wennie"),
        "60165273760",
        REGEXMATCH(LOWER([Duty name]), "junmeng"),
        "60187854378",
        REGEXMATCH(LOWER([Duty name]), "eden"),
        "601126220393",
        REGEXMATCH(LOWER([Duty name]), "mags"),
        "639154652056",
        REGEXMATCH(LOWER([Duty name]), "eugene"),
        "60166629220",
        REGEXMATCH(LOWER([Duty name]), "li wei"),
        "60166500050",
        REGEXMATCH(LOWER([Duty name]), "bryan"),
        "60162000168",
        REGEXMATCH(LOWER([Duty name]), "claire"),
        "60124233295",
        REGEXMATCH(LOWER([Duty name]), "david chin"),
        "60182238665",
        REGEXMATCH(LOWER([Duty name]), "jaye"),
        "60176819892",
        REGEXMATCH(LOWER([Duty name]), "ooi"),
        "60126352981",
        REGEXMATCH(LOWER([Duty name]), "jack"),
        "601169790768",
        REGEXMATCH(LOWER([Duty name]), "rex"),
        "60162122936",
        REGEXMATCH(LOWER([Duty name]), "jerome"),
        "639190041658",
        REGEXMATCH(LOWER([Duty name]), "ronalyn"),
        "639052742067",
        REGEXMATCH(LOWER([Duty name]), "kevin"),
        "639171307765",
        REGEXMATCH(LOWER([Duty name]), "joshua"),
        "639772858019",
        REGEXMATCH(LOWER([Duty name]), "sydney"),
        "639766462190",
        REGEXMATCH(LOWER([Duty name]), "niel"),
        "639455711314",
        REGEXMATCH(LOWER([Duty name]), "don"),
        "639761604020",
        REGEXMATCH(LOWER([Duty name]), "yee wei"),
        "60165197931",
        REGEXMATCH(LOWER([Duty name]), "luigi"),
        "639062839275",
        REGEXMATCH(LOWER([Duty name]), "mark paulo"),
        "639690616051",
        REGEXMATCH(LOWER([Duty name]), "chin fei"),
        "60166038348",
        REGEXMATCH(LOWER([Duty name]), "yong sheng"),
        "60146346671",
        REGEXMATCH(LOWER([Duty name]), "keat vun"),
        "60198800762",
        REGEXMATCH(LOWER([Duty name]), "alvin"),
        "639352838741",
        REGEXMATCH(LOWER([Duty name]), "agustin"),
        "639198613024",
        REGEXMATCH(LOWER([Duty name]), "kelvin"),
        "639171307765"
    )
    """
    
    employees = parse_employee_data(FPMS_DATA, DB_DATA, platformSRE_DATA, cpms_DATA,
                                    pms_DATA, fe_DATA)
    print(f"Loaded {len(employees)} employees.")

    person_intents = generate_person_intents(employees)

    original_intents = {
        "intents": [
            {"tag": "greeting", "patterns": ["Hello", "Hi", "Hey", "How are you", "Good morning"],
             "responses": ["Hello! How can I help you?", "Hi there! What can I do for you?"]},
            {"tag": "goodbye", "patterns": ["Bye", "Goodbye", "See you later"],
             "responses": ["Goodbye! Have a great day!", "See you later!"]},
            {"tag": "thanks", "patterns": ["Thank you", "Thanks", "That's helpful"],
             "responses": ["You're welcome!", "Happy to help!"]}
        ]
    }

    all_intents = original_intents.copy()
    all_intents["intents"].extend(person_intents)
    print(f"\nTotal intents: {len(all_intents['intents'])}")

    # Train the PyTorch model
    trainer = PyTorchTrainer()
    trainer.train(all_intents, output_dir='deep_person_chatbot_pt')

    # Start chatting
    bot = PyTorchChatBot('deep_person_chatbot_pt')
    bot.chat()