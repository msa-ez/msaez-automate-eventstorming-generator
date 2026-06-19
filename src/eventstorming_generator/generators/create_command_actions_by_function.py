from typing import Any, Dict, Optional
from .xml_base import XmlBaseGenerator
from ..models import CreateCommandActionsByFunctionOutput
from ..utils import EsTraceUtil, XmlUtil

class CreateCommandActionsByFunction(XmlBaseGenerator):
    def __init__(self, model_name: str, model_kwargs: Optional[Dict[str, Any]] = None, client: Optional[Dict[str, Any]] = None):
        self.inputs_types_to_check = ["summarizedESValue", "description", "targetBoundedContextName", "targetAggregateName", "eventNamesToGenerate", "commandNamesToGenerate", "readModelNamesToGenerate"]
        super().__init__(model_name, CreateCommandActionsByFunctionOutput, model_kwargs, client)

    def _build_persona_info(self) -> Dict[str, str]:
        return {
            "persona": "Senior Domain-Driven Design Architect and Event-Driven Systems Expert",
            "goal": "To design precise, robust domain models and event-driven architectures that translate complex business requirements into well-structured, maintainable systems following DDD principles and event sourcing patterns. Your primary task is to generate only the specified Commands, Events, and ReadModels for a given Aggregate, based on functional requirements and an existing domain model.",
            "backstory": "With extensive experience implementing complex enterprise systems, I've mastered the practical application of domain-driven design and event-driven architecture patterns. My approach combines technical rigor with pragmatic solutions. I excel at identifying domain boundaries, creating consistent messaging patterns, and designing systems that properly encapsulate business operations. I follow instructions precisely and do not generate any elements that are not explicitly requested."
        }

    def _build_task_instruction_prompt(self) -> str:
        return """<instruction>
    <core_instructions>
        <title>Action Generation Task</title>
        <task_description>Your task is to generate the specified Commands, Events, and ReadModels for a given Aggregate. You must adhere strictly to the list of elements provided and derive their properties from the functional requirements and existing model structure.</task_description>
        
        <critical_constraint>
            <title>CRITICAL: Element Generation Constraint</title>
            <rule>You MUST generate ONLY the elements (Commands, Events, ReadModels) whose names are provided in the `command_names_to_generate`, `event_names_to_generate`, and `read_model_names_to_generate` lists.</rule>
            <rule>Do NOT generate any additional elements, even if they seem plausible from the functional requirements.</rule>
            <rule>If a name from the list does not seem to fit the requirements, generate it anyway with the most logical properties you can infer.</rule>
        </critical_constraint>
        
        <guidelines>
            <title>General Guidelines</title>
            <rule id="1">**Data Types:** Use appropriate Java types (String, Long, Integer, Double, Boolean, Date, List<Type>, etc.). Custom types must be defined as Enumeration or ValueObject within the Aggregate.</rule>
            <rule id="2">**Traceability:** For every created element and its properties, you MUST provide `refs` linking back to the "Functional Requirements". Format is `[[["<start_line_number>", "<start_word_combination>"], ["<end_line_number>", "<end_word_combination>"]]]`. **Phrase Selection:** Choose 2-4 token phrases that align with CLAUSE boundaries — start_phrase = FIRST 2-3 tokens of the substantive content; end_phrase = LAST 2-3 tokens. Do NOT pick single-character phrases or particles that land mid-word or mid-Korean-character-cluster. Never end inside a word. **Skip Structural Lines:** Do NOT anchor on markdown headers (lines starting with `#`, `##`, ..., `######`), table rows (lines starting with `|`), separator lines (`---`), or blank lines. Anchor only on prose lines such as user story narratives (`> *As a ...`), acceptance criteria (`- Given/When/Then ...`), and task entries (`- [PROJ-US-FR-...-TASK-...]`). **Per-Element Specificity:** Each element's refs should point to the SPECIFIC clause that defines THAT element. Do NOT reuse a single broad multi-line block ref for many different commands/events — that destroys per-element traceability. **Zero-Length Forbidden:** start and end positions must NOT be identical. **Span:** Keep refs tight — typically one Given/When/Then block (3 lines) or a single sentence, NOT 20+ line ranges that include unrelated sections.</rule>
            <rule id="3">**Naming and Language:** Technical names (classes, properties) must be in English. Display names (aliases) must be in user's preferred language.</rule>
            <rule id="4">**Naming Patterns:**
                - Commands: Verb + Noun (e.g., CreateOrder)
                - Events: Noun + Past Participle (e.g., OrderCreated)
                - ReadModels: Noun + Purpose (e.g., OrderSummary)
            </rule>
            <rule id="5">**HTTP Verbs:**
                - POST for Create* commands.
                - PUT for Update*/Modify* commands.
                - DELETE for Delete*/Remove* commands.
            </rule>
            <rule id="6">**Command/Event Logic:** Each command must have a corresponding event. Events should contain all relevant data for state changes.</rule>
            <rule id="7">**Avoid:** Do not include comments in the output JSON. Do not create duplicate elements.</rule>
        </guidelines>
    </core_instructions>
    
    <inference_guidelines>
        <title>Inference Guidelines</title>
        <rule id="1">**Directional Focus:** Start by confirming the list of required elements. Your reasoning should explain how you are constructing each of these requested elements based on the provided context.</rule>
        <rule id="2">**Property Derivation:** Justify the properties of each command, event, and read model by linking them to the functional requirements and the existing aggregate structure.</rule>
        <rule id="3">**Traceability:** For each generated element and its properties, determine the `refs` by finding the exact line and word combination in the functional requirements that justifies its creation. This reference must be precise and verifiable.</rule>
        <rule id="4">**Completeness Check:** Conclude by confirming that all requested elements from the input lists have been generated and that no extra elements were created.</rule>
    </inference_guidelines>

    <output_format>
        <title>JSON Output Format</title>
        <description>The output must be a JSON object structured as follows:</description>
        <schema>
{
    "commandActions": [
        {
            "actionName": "<actionName>",
            "objectType": "Command",
            "ids": {
                "aggregateId": "<aggregateId>",
                "commandId": "<commandId>"
            },
            "args": {
                "commandName": "<commandName>",
                "commandAlias": "<commandAlias>",
                "api_verb": "<'POST' | 'PUT' | 'DELETE'>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "properties": [
                    {
                        "name": "<propertyName>",
                        "type": "<propertyType>",
                        "isKey": "<true|false>",
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ],
                "outputEventIds": ["<outputEventId>"],
                "actor": "<actorName>"
            }
        }
    ],
    "eventActions": [
        {
            "actionName": "<actionName>",
            "objectType": "Event",
            "ids": {
                "aggregateId": "<aggregateId>",
                "eventId": "<eventId>"
            },
            "args": {
                "eventName": "<eventName>",
                "eventAlias": "<eventAlias>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "properties": [
                    {
                        "name": "<propertyName>",
                        "type": "<propertyType>",
                        "isKey": "<true|false>",
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ]
            }
        }
    ],
    "readModelActions": [
        {
            "actionName": "<actionName>",
            "objectType": "ReadModel",
            "ids": {
                "aggregateId": "<aggregateId>",
                "readModelId": "<readModelId>"
            },
            "args": {
                "readModelName": "<readModelName>",
                "readModelAlias": "<readModelAlias>",
                "isMultipleResult": "<true|false>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "queryParameters": [
                    {
                        "name": "<propertyName>",
                        "type": "<propertyType>",
                        "isKey": "<true|false>",
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ],
                "actor": "<actorName>"
            }
        }
    ]
}
        </schema>
    </output_format>
</instruction>"""

    def _build_json_example_input_format(self) -> Optional[Dict[str, Any]]:
        description = """# Functional Requirements for Order Management

## User Story
As a customer, I want to place an order for the items in my shopping cart. The system must validate the items, calculate the total price, and confirm my order. Once placed, I should be able to view the order details.

## DDL
```sql
-- Orders Table
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_date DATETIME NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    status ENUM('PENDING', 'CONFIRMED', 'SHIPPED', 'CANCELLED') NOT NULL
);

-- Order Items Table
CREATE TABLE order_items (
    item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);
```"""
        description_with_line_numbers = EsTraceUtil.add_line_numbers_to_description(description)

        return {
            "summarized_existing_event_storming_model": XmlUtil.from_dict({
                "boundedContexts": [{
                    "id": "bc-order",
                    "name": "OrderManagement",
                    "aggregates": [{
                        "id": "agg-order",
                        "name": "Order",
                        "properties": [
                            {"name": "orderId", "type": "Long", "isKey": True},
                            {"name": "customerId", "type": "Long"},
                            {"name": "orderDate", "type": "Date"},
                            {"name": "status", "type": "OrderStatus"}
                        ],
                        "enumerations": [{
                            "id": "enum-order-status",
                            "name": "OrderStatus",
                            "items": ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"]
                        }]
                    }]
                }]
            }),
            "functional_requirements": description_with_line_numbers,
            "target_bounded_context_name": "OrderManagement",
            "target_aggregate_name": "Order",
            "command_names_to_generate": ["PlaceOrder"],
            "event_names_to_generate": ["OrderPlaced"],
            "read_model_names_to_generate": ["OrderDetails"],
            "user_preferred_language": "English"
        }

    def _build_json_example_output_format(self) -> Dict[str, Any]:
        return {
            "commandActions": [{
                "actionName": "PlaceOrderCommand",
                "objectType": "Command",
                "ids": { "aggregateId": "agg-order", "commandId": "cmd-place-order" },
                "args": {
                    "commandName": "PlaceOrder",
                    "commandAlias": "Place Order",
                    "api_verb": "POST",
                    "refs": [[["4", "place an"], ["4", "my order"]]],
                    "properties": [
                        {"name": "customerId", "type": "Long", "isKey": False, "refs": [[["11", "customer_id"], ["11", "NULL"]]]},
                        {"name": "items", "type": "List<OrderItem>", "isKey": False, "refs": [[["4", "items in"], ["4", "cart"]]]}
                    ],
                    "outputEventIds": ["evt-order-placed"],
                    "actor": "Customer"
                }
            }],
            "eventActions": [{
                "actionName": "OrderPlacedEvent",
                "objectType": "Event",
                "ids": { "aggregateId": "agg-order", "eventId": "evt-order-placed" },
                "args": {
                    "eventName": "OrderPlaced",
                    "eventAlias": "Order Placed",
                    "refs": [[["4", "an order"], ["4", "confirm"]]],
                    "properties": [
                        {"name": "orderId", "type": "Long", "isKey": True, "refs": [[["10", "order_id"], ["10", "KEY"]]]},
                        {"name": "customerId", "type": "Long", "isKey": False, "refs": [[["11", "customer_id"], ["11", "NULL"]]]},
                        {"name": "orderDate", "type": "Date", "isKey": False, "refs": [[["12", "order_date"], ["12", "NULL"]]]},
                        {"name": "totalPrice", "type": "Double", "isKey": False, "refs": [[["13", "total_price"], ["13", "NULL"]]]},
                        {"name": "status", "type": "OrderStatus", "isKey": False, "refs": [[["14", "status"], ["14", "NULL"]]]},
                        {"name": "items", "type": "List<OrderItem>", "isKey": False, "refs": [[["18", "TABLE"], ["18", "items"]]]}
                    ]
                }
            }],
            "readModelActions": [{
                "actionName": "OrderDetailsReadModel",
                "objectType": "ReadModel",
                "ids": { "aggregateId": "agg-order", "readModelId": "read-order-details" },
                "args": {
                    "readModelName": "OrderDetails",
                    "readModelAlias": "View Order Details",
                    "isMultipleResult": False,
                    "refs": [[["4", "view"], ["4", "details"]]],
                    "queryParameters": [
                        {"name": "orderId", "type": "Long", "isKey": True, "refs": [[["10", "order_id"], ["10", "KEY"]]]}
                    ],
                    "actor": "Customer"
                }
            }]
        }
    
    def _build_json_user_query_input_format(self) -> Dict[str, Any]:
        inputs = self.client.get("inputs")

        return {
            "summarized_existing_event_storming_model": XmlUtil.from_dict(inputs.get("summarizedESValue")),
            "functional_requirements": inputs.get("description"),
            "target_bounded_context_name": inputs.get("targetBoundedContextName"),
            "target_aggregate_name": inputs.get("targetAggregateName"),
            "command_names_to_generate": inputs.get("commandNamesToGenerate"),
            "event_names_to_generate": inputs.get("eventNamesToGenerate"),
            "read_model_names_to_generate": inputs.get("readModelNamesToGenerate"),
            "user_preferred_language": self.client.get("preferredLanguage")
        }