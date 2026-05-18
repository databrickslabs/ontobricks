"""OntologyPatternToolkit — 19-pitfall detector (P1.1–P4.7).

Vendored from https://github.com/D2KLab/Ontology-Pitfalls-Detector (Apache-2.0).
Heavy ML deps (sentence-transformers, scikit-learn, nltk) are imported at module
level via try/except so taxonomy constants are always accessible even when the
optional ``pitfalls`` extra is not installed.
"""
from __future__ import annotations

from collections import Counter
import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# rdflib is always available (core dep)
from rdflib import Graph, OWL, RDF, RDFS, URIRef

# Optional ML deps — imported lazily; None when the pitfalls extra is not installed.
try:
    import numpy as np
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    _DEPS_AVAILABLE = True
except ImportError:
    np = None  # type: ignore[assignment]
    SentenceTransformer = None  # type: ignore[assignment,misc]
    cosine_similarity = None  # type: ignore[assignment]
    _DEPS_AVAILABLE = False

from .utils import (
    camel_case_split,
    ensure_nltk_resource,
    extract_label,
    flatten,
    normalize_name,
    parse_pattern_selection,
    sort_pattern_ids,
)

PITFALL_TAXONOMY = (
    {
        "category": "Logical Issues",
        "pitfall_id": "P1.1",
        "title": "Parent disjoint with children",
        "description": (
            "A class is declared disjoint from one of its own subclasses. "
            "This is a contradiction: no individual can simultaneously belong "
            "to both a class and its subclass if they are disjoint, making the "
            "subclass unsatisfiable (always empty)."
        ),
    },
    {
        "category": "Logical Issues",
        "pitfall_id": "P1.2",
        "title": "Entity as subclass of both parent and grandparent",
        "description": (
            "A class is explicitly declared as a direct subclass of both a "
            "parent class and one of that parent's ancestors. The declaration "
            "to the ancestor is redundant because subclass transitivity already "
            "implies it, and it can mislead reasoners."
        ),
    },
    {
        "category": "Logical Issues",
        "pitfall_id": "P1.3",
        "title": "Logical inconsistencies",
        "description": (
            "Axioms in the ontology produce unsatisfiable classes — e.g., a "
            "class is simultaneously defined as a subclass of two disjoint "
            "classes, or a restriction forces contradictory types. An OWL "
            "reasoner would flag these classes as equivalent to owl:Nothing."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.1",
        "title": "Not connected hierarchies",
        "description": (
            "The ontology contains isolated class trees with no common root "
            "other than owl:Thing. Disconnected hierarchies often indicate "
            "that separate sub-ontologies were merged without alignment, or "
            "that top-level grouping concepts are missing."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.2",
        "title": "Single subclass parent",
        "description": (
            "A class has exactly one direct subclass. This is often a sign "
            "of unnecessary intermediate nodes: if a parent has only one child, "
            "the hierarchy could usually be flattened without loss of semantics, "
            "unless the parent is used in axioms independently."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.3",
        "title": "Superfluous disjointness",
        "description": (
            "Two classes are declared disjoint when one is already a subclass "
            "of the other, or when disjointness is already implied by the "
            "hierarchy. The explicit disjointness assertion is redundant and "
            "may create unintended logical side-effects."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.4",
        "title": "Single subproperty parent",
        "description": (
            "A property has exactly one direct sub-property. Mirrors P2.2 "
            "for properties: if a property hierarchy node has only one child, "
            "it may be a superfluous intermediate that adds no modelling value."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.5",
        "title": "Range/Domain expansion",
        "description": (
            "A sub-property declares a domain or range that is broader than "
            "its parent property's domain/range. This violates the inheritance "
            "contract: a sub-property should restrict (narrow) rather than "
            "expand the domain/range it inherits."
        ),
    },
    {
        "category": "Structural Issues",
        "pitfall_id": "P2.6",
        "title": "Possible hierarchy among properties",
        "description": (
            "Two or more properties have names so similar that one may be a "
            "specialisation of the other, yet no rdfs:subPropertyOf link is "
            "declared between them. This check flags candidate pairs worth "
            "reviewing for a missing sub-property relationship."
        ),
    },
    {
        "category": "Redundancy / Naming Issues",
        "pitfall_id": "P3.1",
        "title": "Properties replicating standard RDF ones",
        "description": (
            "The ontology defines custom properties that duplicate well-known "
            "RDF/RDFS/OWL vocabulary (e.g., a custom 'hasLabel' property when "
            "rdfs:label already exists). Using standard vocabulary improves "
            "interoperability and avoids redundant machinery."
        ),
    },
    {
        "category": "Redundancy / Naming Issues",
        "pitfall_id": "P3.2",
        "title": "Range in property title",
        "description": (
            "A property name encodes its range type (e.g., 'hasPersonName', "
            "'containsEvent'). Embedding the range in the label couples naming "
            "to structure: renaming or changing the range requires renaming "
            "the property, and it conflates two distinct modelling concerns."
        ),
    },
    {
        "category": "Redundancy / Naming Issues",
        "pitfall_id": "P3.3",
        "title": "Domain in property title",
        "description": (
            "A property name encodes its domain class (e.g., 'personHasName', "
            "'orderContainsItem'). Same fragility as P3.2: naming should "
            "express the relationship semantics, not repeat structural "
            "information already captured by rdfs:domain declarations."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.1",
        "title": "Overly generic classes",
        "description": (
            "Classes whose labels are very broad, generic terms (e.g., "
            "'Thing', 'Entity', 'Object', 'Item') that carry little semantic "
            "specificity. These often indicate placeholder concepts that were "
            "never refined or that model a concept already covered by owl:Thing."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.2",
        "title": "Synonyms in superclasses",
        "description": (
            "A class has multiple superclasses whose labels are synonyms or "
            "near-synonyms (detected via WordNet). Declaring redundant synonymous "
            "parents inflates the hierarchy without adding distinctions, and may "
            "cause unintended logical entailments. Requires ML dependencies."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.3",
        "title": "Conflicting hierarchy",
        "description": (
            "Sibling classes (sharing the same parent) have labels that are "
            "antonyms of each other (detected via WordNet/sentiment analysis). "
            "Antonymous siblings often indicate a modelling conflict or that "
            "the classes should instead be declared disjoint explicitly. Requires ML dependencies."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.4",
        "title": "Subclasses with same semantics as superclasses",
        "description": (
            "A subclass has a name or label that is semantically nearly "
            "identical to its parent (measured via sentence-transformer "
            "embeddings). If the subclass adds no new semantics, the "
            "hierarchy level is unnecessary. Requires ML dependencies."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.5",
        "title": "Synonyms in properties",
        "description": (
            "Two or more properties in the ontology have labels that are "
            "synonyms or highly semantically similar (via embeddings), "
            "suggesting they may model the same relationship and one could "
            "be merged or aliased via owl:equivalentProperty. Requires ML dependencies."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.6",
        "title": "Inverse properties not declared",
        "description": (
            "A pair of properties appears to be semantically inverse "
            "(e.g., 'hasPart' / 'isPartOf') but no owl:inverseOf axiom "
            "links them. Declaring inverses allows reasoners to infer "
            "both directions automatically. Requires ML dependencies."
        ),
    },
    {
        "category": "Semantic Issues",
        "pitfall_id": "P4.7",
        "title": "DataProperties that can become ObjectProperties",
        "description": (
            "A datatype property has values that look like class names or "
            "identifiers of existing classes (e.g., a 'hasType' string "
            "property whose values match class labels). These are candidates "
            "for promotion to object properties pointing to the named classes."
        ),
    },
)

PITFALL_RUN_METHODS = {
    "P1.1": "run_p1_1",
    "P1.2": "run_p1_2",
    "P1.3": "run_p1_3",
    "P2.1": "run_p2_1",
    "P2.2": "run_p2_2",
    "P2.3": "run_p2_3",
    "P2.4": "run_p2_4",
    "P2.5": "run_p2_5",
    "P2.6": "run_p2_6",
    "P3.1": "run_p3_1",
    "P3.2": "run_p3_2",
    "P3.3": "run_p3_3",
    "P4.1": "run_p4_1",
    "P4.2": "run_p4_2",
    "P4.3": "run_p4_3",
    "P4.4": "run_p4_4",
    "P4.5": "run_p4_5",
    "P4.6": "run_p4_6",
    "P4.7": "run_p4_7",
}

PITFALL_BY_ID = {entry["pitfall_id"]: entry for entry in PITFALL_TAXONOMY}


class OntologyPatternToolkit:
    PATTERN_METHODS = dict(PITFALL_RUN_METHODS)

    def __init__(self, ontology_path: str, model_name: str = "all-MiniLM-L6-v2") -> None:
        if not _DEPS_AVAILABLE:
            raise ImportError(
                "Pitfall detection requires optional dependencies. "
                "Install with: pip install .[pitfalls]"
            )

        self.ontology_path = Path(ontology_path).expanduser().resolve()
        if not self.ontology_path.exists():
            raise FileNotFoundError(f"Ontology file not found: {self.ontology_path}")

        self.graph = Graph()
        self.graph.parse(str(self.ontology_path))

        all_classes = list(self.graph.subjects(RDF.type, OWL.Class)) + list(self.graph.subjects(RDF.type, RDFS.Class))
        self.oclasses = sorted(
            [c for c in all_classes if isinstance(c, URIRef)],
            key=str,
        )

        self.oobjprops = sorted(
            [p for p in self.graph.subjects(RDF.type, OWL.ObjectProperty) if isinstance(p, URIRef)],
            key=str,
        )
        self.odataprops = sorted(
            [p for p in self.graph.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)],
            key=str,
        )
        rdf_properties = set(self.graph.subjects(RDF.type, RDF.Property))
        rdf_properties_range = [list(self.graph.objects(r, RDFS.range))[0] for r in rdf_properties]
        self.oobjprops += [r[0] for r in zip(rdf_properties, rdf_properties_range) if isinstance(r[1], URIRef)]
        self.odataprops += [r for r in rdf_properties if r not in self.oobjprops]

        self.all_props = self.oobjprops + self.odataprops

        self.model_name = model_name
        self._model: Optional[SentenceTransformer] = None
        self._class_similarity_cache: Optional[Dict[str, Any]] = None
        self._property_similarity_cache: Optional[Dict[str, Any]] = None

    @classmethod
    def available_patterns(cls) -> List[str]:
        return sort_pattern_ids([entry["pitfall_id"] for entry in PITFALL_TAXONOMY])

    @classmethod
    def pitfall_taxonomy(cls) -> List[Dict[str, str]]:
        return [dict(entry) for entry in PITFALL_TAXONOMY]

    @classmethod
    def normalize_pitfall_id(cls, raw_id: str) -> str:
        token = str(raw_id).strip().upper().rstrip(".")
        if not token:
            raise ValueError("Pitfall identifier cannot be empty.")

        if token == "ALL":
            return token

        if token not in PITFALL_BY_ID:
            options = ", ".join(cls.available_patterns())
            raise ValueError(f"Unknown pitfall '{raw_id}'. Available: {options}")

        return token

    def metadata(self) -> Dict[str, Any]:
        return {
            "ontology_path": str(self.ontology_path),
            "classes": len(self.oclasses),
            "object_properties": len(self.oobjprops),
            "datatype_properties": len(self.odataprops),
        }

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def _build_text_embedding_cache(self, texts: Sequence[str]) -> Dict[str, np.ndarray]:
        unique_texts = sorted(set(text for text in texts if text))
        if not unique_texts:
            return {}

        embeddings = self._get_model().encode(unique_texts, show_progress_bar=False)
        return {text: embeddings[idx] for idx, text in enumerate(unique_texts)}

    def _embedding_for(self, text: str, cache: Dict[str, np.ndarray]) -> np.ndarray:
        if text not in cache:
            cache[text] = self._get_model().encode([text], show_progress_bar=False)[0]
        return cache[text]

    def _text_similarity(self, text_a: str, text_b: str, cache: Dict[str, np.ndarray]) -> float:
        emb_a = self._embedding_for(text_a, cache)
        emb_b = self._embedding_for(text_b, cache)
        return float(cosine_similarity(emb_a.reshape(1, -1), emb_b.reshape(1, -1))[0][0])

    def _class_similarity_context(self) -> Dict[str, Any]:
        if self._class_similarity_cache is not None:
            return self._class_similarity_cache

        class_labels = [extract_label(c, clean=True) for c in self.oclasses]
        class_descriptions: List[str] = []
        for cls_uri in self.oclasses:
            comments = list(self.graph.objects(cls_uri, RDFS.comment))
            class_descriptions.append(str(comments[0]) if comments else "")

        label_embeddings = self._get_model().encode(class_labels, show_progress_bar=False)
        description_embeddings = self._get_model().encode(class_descriptions, show_progress_bar=False)

        label_similarity = cosine_similarity(label_embeddings)
        description_similarity = cosine_similarity(description_embeddings)
        combined_similarity = 0.4 * label_similarity + 0.6 * description_similarity

        self._class_similarity_cache = {
            "class_labels": class_labels,
            "class_descriptions": class_descriptions,
            "label_similarity": label_similarity,
            "description_similarity": description_similarity,
            "combined_similarity": combined_similarity,
        }
        return self._class_similarity_cache

    def _property_similarity_context(self) -> Dict[str, Any]:
        if self._property_similarity_cache is not None:
            return self._property_similarity_cache

        prop_types = {
            p: ("ObjectProperty" if p in self.oobjprops else "DatatypeProperty")
            for p in self.all_props
        }
        prop_labels = [extract_label(p, clean=True) for p in self.all_props]

        prop_descriptions = []
        for p in self.all_props:
            comments = list(self.graph.objects(p, RDFS.comment))
            prop_descriptions.append(str(comments[0]) if comments else "")

        if not self.all_props:
            self._property_similarity_cache = {
                "prop_types": prop_types,
                "pairs": [],
                "quantiles": {},
            }
            return self._property_similarity_cache

        p10_label_emb = self._get_model().encode(prop_labels, show_progress_bar=False)
        p10_desc_emb = self._get_model().encode(prop_descriptions, show_progress_bar=False)

        p10_label_sim = cosine_similarity(p10_label_emb)
        p10_desc_sim = cosine_similarity(p10_desc_emb)
        p10_combined_sim = 0.7 * p10_label_sim + 0.3 * p10_desc_sim

        pairs: List[Dict[str, Any]] = []
        n_props = len(self.all_props)
        for i in range(n_props):
            for j in range(i + 1, n_props):
                pairs.append(
                    {
                        "p1_uri": self.all_props[i],
                        "p2_uri": self.all_props[j],
                        "p1_label": prop_labels[i],
                        "p2_label": prop_labels[j],
                        "p1_type": prop_types[self.all_props[i]],
                        "p2_type": prop_types[self.all_props[j]],
                        "label_similarity": float(p10_label_sim[i, j]),
                        "description_similarity": float(p10_desc_sim[i, j]),
                        "combined_similarity": float(p10_combined_sim[i, j]),
                    }
                )

        all_scores = np.array([pair["combined_similarity"] for pair in pairs], dtype=float)
        quantiles = {}
        if len(all_scores) > 0:
            for q in [0.90, 0.95, 0.98, 0.99]:
                quantiles[f"q{int(q * 100)}"] = float(np.quantile(all_scores, q))

        self._property_similarity_cache = {
            "prop_types": prop_types,
            "pairs": pairs,
            "quantiles": quantiles,
        }
        return self._property_similarity_cache

    def _p10_candidates(self, threshold: float) -> List[Dict[str, Any]]:
        ctx = self._property_similarity_context()
        candidates = []

        for pair in ctx["pairs"]:
            if pair["combined_similarity"] < threshold:
                continue

            p1 = pair["p1_uri"]
            p2 = pair["p2_uri"]
            if (p1, OWL.inverseOf, p2) in self.graph or (p2, OWL.inverseOf, p1) in self.graph:
                continue

            candidates.append(pair)

        return sorted(candidates, key=lambda x: x["combined_similarity"], reverse=True)

    def _property_domain_range(self) -> Tuple[Dict[URIRef, Set[Any]], Dict[URIRef, Set[Any]]]:
        prop_domain: Dict[URIRef, Set[Any]] = {}
        prop_range: Dict[URIRef, Set[Any]] = {}

        for prop in self.all_props:
            prop_domain[prop] = set(self.graph.objects(prop, RDFS.domain))
            prop_range[prop] = set(self.graph.objects(prop, RDFS.range))

        return prop_domain, prop_range

    def _labels_from_uris(self, uris: Set[URIRef]) -> List[str]:
        return [extract_label(uri, clean=True) for uri in sorted(uris, key=str)]

    def _get_antonyms_and_parents(self, term: str) -> List[Dict[str, str]]:
        from nltk.corpus import wordnet as wn

        antonyms: List[Dict[str, str]] = []
        syns = wn.synsets(term.lower().replace(" ", "_"))

        for syn in syns:
            for lemma in syn.lemmas():
                for ant_lemma in lemma.antonyms():
                    ant_name = ant_lemma.name().replace("_", " ")
                    for hyp in ant_lemma.synset().hypernyms():
                        parent_name = hyp.lemmas()[0].name().replace("_", " ")
                        antonyms.append({"antonym": ant_name, "parent": parent_name})
                        break

        return antonyms

    def _are_contrasting(
        self,
        l1: str,
        l2: str,
        embedding_cache: Dict[str, np.ndarray],
        sentiment_analyzer: SentimentIntensityAnalyzer,
        sim_threshold: float,
        polarity_threshold: float,
    ) -> Dict[str, Any]:
        sim = self._text_similarity(l1, l2, embedding_cache)
        sent_l1 = sentiment_analyzer.polarity_scores(l1)["compound"]
        sent_l2 = sentiment_analyzer.polarity_scores(l2)["compound"]
        polarity_dist = abs(sent_l1 - sent_l2) / 2.0

        if polarity_dist >= polarity_threshold:
            return {
                "is_contrasting": True,
                "reason": "opposite polarity",
                "similarity": sim,
                "polarity_distance": polarity_dist,
            }

        if sim < sim_threshold:
            return {
                "is_contrasting": True,
                "reason": "low similarity",
                "similarity": sim,
                "polarity_distance": polarity_dist,
            }

        antonyms = self._get_antonyms_and_parents(l1)
        for antonym_data in antonyms:
            sim_ant_l2 = self._text_similarity(antonym_data["antonym"], l2, embedding_cache)
            if sim_ant_l2 > sim_threshold:
                return {
                    "is_contrasting": True,
                    "reason": f"antonym '{antonym_data['antonym']}' similar to second label",
                    "similarity": sim,
                    "polarity_distance": polarity_dist,
                }

        return {
            "is_contrasting": False,
            "reason": "synonymous or unrelated",
            "similarity": sim,
            "polarity_distance": polarity_dist,
        }

    def run_pattern(self, pattern_id: str) -> Dict[str, Any]:
        pitfall_id = self.normalize_pitfall_id(pattern_id)
        if pitfall_id == "ALL":
            raise ValueError("Use run_all() or run_patterns(['all']) to run all pitfalls.")

        method_name = self.PATTERN_METHODS.get(pitfall_id)
        if method_name is None:
            available = ", ".join(self.available_patterns())
            raise ValueError(f"Unknown pitfall '{pattern_id}'. Available: {available}")

        return getattr(self, method_name)()

    def run_patterns(self, pattern_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        selected_pitfalls = parse_pattern_selection(
            pattern_ids,
            self.available_patterns(),
            normalizer=self.normalize_pitfall_id,
        )

        return {pitfall_id: self.run_pattern(pitfall_id) for pitfall_id in selected_pitfalls}

    def run_all(self) -> Dict[str, Dict[str, Any]]:
        return self.run_patterns(["all"])

    def run_p4_1(self) -> Dict[str, Any]:
        ensure_nltk_resource("corpora/wordnet", "wordnet")
        from nltk.corpus import wordnet as wn

        concept_synsets = wn.synsets("concept")
        if not concept_synsets:
            return {
                "count": 0,
                "items": [],
                "warning": "WordNet synset 'concept' not available.",
            }

        wn_concept = concept_synsets[0]
        wn_entity = wn.synset("entity.n.01")
        wn_abs_entity = wn.synset("abstraction.n.06")

        superclasses = [
            c for c in self.oclasses if any(True for _ in self.graph.subjects(RDFS.subClassOf, c))
        ]

        scores = []
        for cls_uri in superclasses:
            tokens = camel_case_split(extract_label(cls_uri))
            if not tokens:
                continue

            long_word = "_".join(tokens)
            synsets = wn.synsets(long_word)  # noqa: F821 — imported above
            if not synsets:
                synsets = wn.synsets(tokens[-1])
            if not synsets:
                continue

            hpath = list(reversed(synsets[0].hypernym_paths()[0]))

            distance_from_concept = hpath.index(wn_concept) if wn_concept in hpath else 999
            distance_from_entity = hpath.index(wn_entity) if wn_entity in hpath else 999
            distance_from_abs_entity = hpath.index(wn_abs_entity) if wn_abs_entity in hpath else 999

            score = min(distance_from_concept, distance_from_entity, distance_from_abs_entity)
            if int(score) > 2:
                continue
            scores.append(
                {
                    "class_uri": str(cls_uri),
                    "class_label": "".join(tokens),
                    "tokens": tokens,
                    "distance": int(score),
                }
            )

        scores = sorted(scores, key=lambda x: x["distance"])

        buckets: Dict[str, List[str]] = {}
        for item in scores:
            bucket = str(item["distance"])
            buckets.setdefault(bucket, []).append(item["class_label"])

        ratio = len(scores) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "count": len(scores),
            "ratio": ratio,
            "items": scores,
            "distance_buckets": buckets,
        }

    def run_p2_1(self) -> Dict[str, Any]:
        split_labels = {
            cls_uri: camel_case_split(extract_label(cls_uri))
            for cls_uri in self.oclasses
        }

        mismatches: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str]] = set()

        for word, count in Counter(flatten(split_labels.values())).most_common():
            if count == 1:
                break

            involved = [
                (uri, "".join(tokens))
                for uri, tokens in split_labels.items()
                if word in tokens
            ]
            if len(involved) < 2:
                continue

            root_uri, root_label = sorted(involved, key=lambda x: len(x[1]))[0]

            for uri, label in involved:
                if label == root_label:
                    continue
                if not label.endswith(root_label):
                    continue

                connected = root_uri in set(self.graph.transitive_objects(uri, RDFS.subClassOf))
                if connected:
                    continue

                key = (str(uri), str(root_uri))
                if key in seen:
                    continue

                seen.add(key)
                mismatches.append(
                    {
                        "class_uri": str(uri),
                        "class_label": label,
                        "missing_parent_uri": str(root_uri),
                        "missing_parent_label": root_label,
                        "shared_token": word,
                    }
                )

        ratio = len(mismatches) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "count": len(mismatches),
            "ratio": ratio,
            "items": mismatches,
        }

    def run_p4_2(self, threshold: float = 0.8, top_k_per_class: int = 3) -> Dict[str, Any]:
        ctx = self._class_similarity_context()
        label_sim = ctx["label_similarity"]
        desc_sim = ctx["description_similarity"]
        combined_sim = ctx["combined_similarity"]

        items: List[Dict[str, Any]] = []

        for i, cls_uri in enumerate(self.oclasses):
            class_matches = []
            for j in range(i + 1, len(self.oclasses)):
                score = float(combined_sim[i, j])
                if score < threshold:
                    continue

                class_matches.append(
                    {
                        "class_1_uri": str(cls_uri),
                        "class_1_label": extract_label(cls_uri, clean=True),
                        "class_2_uri": str(self.oclasses[j]),
                        "class_2_label": extract_label(self.oclasses[j], clean=True),
                        "label_similarity": float(label_sim[i, j]),
                        "description_similarity": float(desc_sim[i, j]),
                        "combined_similarity": score,
                    }
                )

            class_matches.sort(key=lambda x: x["combined_similarity"], reverse=True)
            if top_k_per_class > 0:
                class_matches = class_matches[:top_k_per_class]
            items.extend(class_matches)

        items.sort(key=lambda x: x["combined_similarity"], reverse=True)

        ratio = len(items) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "threshold": threshold,
            "count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p1_1(self) -> Dict[str, Any]:
        results = []
        seen = set()

        for child, parent in self.graph.subject_objects(RDFS.subClassOf):
            if not isinstance(child, URIRef) or not isinstance(parent, URIRef):
                continue

            is_disjoint = (parent, OWL.disjointWith, child) in self.graph or (
                child,
                OWL.disjointWith,
                parent,
            ) in self.graph
            if not is_disjoint:
                continue

            key = (str(parent), str(child))
            if key in seen:
                continue
            seen.add(key)

            results.append(
                {
                    "parent_uri": str(parent),
                    "parent_label": extract_label(parent, clean=True),
                    "child_uri": str(child),
                    "child_label": extract_label(child, clean=True),
                }
            )

        ratio = len(results) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "count": len(results),
            "ratio": ratio,
            "items": results,
        }

    def run_p2_2(self) -> Dict[str, Any]:
        superclasses = [
            c for c in self.oclasses if any(True for _ in self.graph.subjects(RDFS.subClassOf, c))
        ]

        singlechild_superclasses = [
            c for c in superclasses if sum(1 for _ in self.graph.subjects(RDFS.subClassOf, c)) == 1
        ]

        items = [
            {
                "class_uri": str(cls_uri),
                "class_label": extract_label(cls_uri, clean=True),
            }
            for cls_uri in singlechild_superclasses
        ]

        ratio = len(items) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p2_3(self, min_similarity: float = 0.6, top_k: int = 20) -> Dict[str, Any]:
        pairs: List[Tuple[URIRef, URIRef]] = []
        seen = set()

        for c1, c2 in self.graph.subject_objects(OWL.disjointWith):
            if not isinstance(c1, URIRef) or not isinstance(c2, URIRef):
                continue
            if c1 == c2:
                continue

            ordered = tuple(sorted((c1, c2), key=str))
            key = (str(ordered[0]), str(ordered[1]))
            if key in seen:
                continue

            seen.add(key)
            pairs.append(ordered)

        labels_for_cache = []
        for c1, c2 in pairs:
            labels_for_cache.append(extract_label(c1, clean=True))
            labels_for_cache.append(extract_label(c2, clean=True))

        embedding_cache = self._build_text_embedding_cache(labels_for_cache)

        filtered = []
        for c1, c2 in pairs:
            l1 = extract_label(c1, clean=True)
            l2 = extract_label(c2, clean=True)
            sim = self._text_similarity(l1, l2, embedding_cache)
            if sim < min_similarity:
                continue

            filtered.append(
                {
                    "class_1_uri": str(c1),
                    "class_1_label": l1,
                    "class_2_uri": str(c2),
                    "class_2_label": l2,
                    "label_similarity": sim,
                }
            )

        filtered = sorted(filtered, key=lambda x: x["label_similarity"], reverse=True)
        if top_k > 0:
            filtered = filtered[:top_k]

        ratio = len(filtered) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "min_similarity": min_similarity,
            "count": len(filtered),
            "ratio": ratio,
            "items": filtered,
        }

    def run_p4_3(
        self,
        sim_threshold: float = 0.1,
        polarity_threshold: float = 0.3,
    ) -> Dict[str, Any]:
        ensure_nltk_resource("corpora/wordnet", "wordnet")
        ensure_nltk_resource("sentiment/vader_lexicon", "vader_lexicon")
        from nltk.sentiment import SentimentIntensityAnalyzer

        parent_child = []
        texts = []

        for child, parent in self.graph.subject_objects(RDFS.subClassOf):
            if not isinstance(child, URIRef) or not isinstance(parent, URIRef):
                continue

            parent_label = extract_label(parent, clean=True)
            child_label = extract_label(child, clean=True)
            parent_child.append((parent, child, parent_label, child_label))
            texts.extend([parent_label, child_label])

        embedding_cache = self._build_text_embedding_cache(texts)
        sentiment_analyzer = SentimentIntensityAnalyzer()

        items = []
        for parent, child, parent_label, child_label in parent_child:
            contrast = self._are_contrasting(
                parent_label,
                child_label,
                embedding_cache,
                sentiment_analyzer,
                sim_threshold=sim_threshold,
                polarity_threshold=polarity_threshold,
            )
            if not contrast["is_contrasting"]:
                continue

            items.append(
                {
                    "parent_uri": str(parent),
                    "parent_label": parent_label,
                    "child_uri": str(child),
                    "child_label": child_label,
                    "reason": contrast["reason"],
                    "similarity": contrast["similarity"],
                    "polarity_distance": contrast["polarity_distance"],
                }
            )

        ratio = len(items) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "sim_threshold": sim_threshold,
            "polarity_threshold": polarity_threshold,
            "count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p4_4(self, sim_threshold: float = 0.8) -> Dict[str, Any]:
        ctx = self._class_similarity_context()
        combined_sim = ctx["combined_similarity"]
        uri_to_idx = {uri: idx for idx, uri in enumerate(self.oclasses)}

        parent_child_pairs = [
            (parent, child)
            for child, parent in self.graph.subject_objects(RDFS.subClassOf)
            if isinstance(child, URIRef) and isinstance(parent, URIRef)
        ]

        domain_props_by_class = {c: set() for c in self.oclasses}
        range_props_by_class = {c: set() for c in self.oclasses}

        for prop in self.all_props:
            for domain_cls in self.graph.objects(prop, RDFS.domain):
                if domain_cls in domain_props_by_class:
                    domain_props_by_class[domain_cls].add(prop)
            for range_cls in self.graph.objects(prop, RDFS.range):
                if range_cls in range_props_by_class:
                    range_props_by_class[range_cls].add(prop)

        candidates = []
        for parent, child in parent_child_pairs:
            if parent not in uri_to_idx or child not in uri_to_idx:
                continue

            i = uri_to_idx[parent]
            j = uri_to_idx[child]
            sim = float(combined_sim[i, j])
            if sim < sim_threshold:
                continue

            shared_domain = domain_props_by_class[parent].intersection(domain_props_by_class[child])
            shared_range = range_props_by_class[parent].intersection(range_props_by_class[child])

            candidates.append(
                {
                    "parent_uri": str(parent),
                    "child_uri": str(child),
                    "parent_label": extract_label(parent, clean=True),
                    "child_label": extract_label(child, clean=True),
                    "semantic_similarity": sim,
                    "shared_domain_count": len(shared_domain),
                    "shared_range_count": len(shared_range),
                    "shared_domain_props": self._labels_from_uris(shared_domain),
                    "shared_range_props": self._labels_from_uris(shared_range),
                }
            )

        candidates = sorted(candidates, key=lambda x: x["semantic_similarity"], reverse=True)

        ratio = len(candidates) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "sim_threshold": sim_threshold,
            "count": len(candidates),
            "ratio": ratio,
            "items": candidates,
        }

    def run_p1_2(self) -> Dict[str, Any]:
        triples = set()

        for child, parent in self.graph.subject_objects(RDFS.subClassOf):
            if not isinstance(child, URIRef) or not isinstance(parent, URIRef):
                continue

            for grandparent in self.graph.objects(parent, RDFS.subClassOf):
                if not isinstance(grandparent, URIRef):
                    continue

                if child == parent or parent == grandparent or child == grandparent:
                    continue

                if (child, RDFS.subClassOf, grandparent) in self.graph:
                    triples.add((child, parent, grandparent))

        items = [
            {
                "child_uri": str(child),
                "parent_uri": str(parent),
                "grandparent_uri": str(grandparent),
                "child_label": extract_label(child, clean=True),
                "parent_label": extract_label(parent, clean=True),
                "grandparent_label": extract_label(grandparent, clean=True),
            }
            for child, parent, grandparent in sorted(triples, key=lambda x: (str(x[0]), str(x[1]), str(x[2])))
        ]

        ratio = len(items) / len(self.oclasses) if self.oclasses else 0.0
        return {
            "count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p4_5(self, threshold: float = 0.8, max_results: int = 50) -> Dict[str, Any]:
        ctx = self._property_similarity_context()
        candidates = self._p10_candidates(threshold)

        if max_results > 0:
            visible = candidates[:max_results]
        else:
            visible = candidates

        items = [
            {
                "p1_uri": str(x["p1_uri"]),
                "p2_uri": str(x["p2_uri"]),
                "p1_label": x["p1_label"],
                "p2_label": x["p2_label"],
                "p1_type": x["p1_type"],
                "p2_type": x["p2_type"],
                "label_similarity": x["label_similarity"],
                "description_similarity": x["description_similarity"],
                "combined_similarity": x["combined_similarity"],
            }
            for x in visible
        ]

        ratio = len(items) / len(self.all_props) if self.all_props else 0.0
        return {
            "threshold": threshold,
            "quantiles": ctx["quantiles"],
            "total_count": len(candidates),
            "returned_count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p4_6(self, threshold: float = 0.8) -> Dict[str, Any]:
        prop_domain, prop_range = self._property_domain_range()
        threshold_candidates = self._p10_candidates(threshold)

        potential_inverses = []
        for candidate in threshold_candidates:
            p1_uri = candidate["p1_uri"]
            p2_uri = candidate["p2_uri"]

            d1 = prop_domain.get(p1_uri, set())
            r1 = prop_range.get(p1_uri, set())
            d2 = prop_domain.get(p2_uri, set())
            r2 = prop_range.get(p2_uri, set())

            domain_swapped = d1 == r2 and r1 == d2 and bool(d1) and bool(r1)
            partial_inverse = (d1 == r2 or r1 == d2) and bool(d1) and bool(r1)

            if domain_swapped or partial_inverse:
                potential_inverses.append(
                    {
                        "p1_uri": str(p1_uri),
                        "p2_uri": str(p2_uri),
                        "p1_label": candidate["p1_label"],
                        "p2_label": candidate["p2_label"],
                        "combined_similarity": candidate["combined_similarity"],
                        "domain_swapped": domain_swapped,
                        "p1_domain": self._labels_from_uris({x for x in d1 if isinstance(x, URIRef)}),
                        "p1_range": self._labels_from_uris({x for x in r1 if isinstance(x, URIRef)}),
                        "p2_domain": self._labels_from_uris({x for x in d2 if isinstance(x, URIRef)}),
                        "p2_range": self._labels_from_uris({x for x in r2 if isinstance(x, URIRef)}),
                    }
                )

        ratio = len(potential_inverses) / len(self.all_props) if self.all_props else 0.0
        return {
            "threshold": threshold,
            "count": len(potential_inverses),
            "ratio": ratio,
            "items": potential_inverses,
        }

    def run_p2_6(self, threshold: float = 0.8) -> Dict[str, Any]:
        prop_domain, prop_range = self._property_domain_range()
        threshold_candidates = self._p10_candidates(threshold)

        potential_siblings = []
        for candidate in threshold_candidates:
            p1_uri = candidate["p1_uri"]
            p2_uri = candidate["p2_uri"]

            d1 = prop_domain.get(p1_uri, set())
            r1 = prop_range.get(p1_uri, set())
            d2 = prop_domain.get(p2_uri, set())
            r2 = prop_range.get(p2_uri, set())

            domain_matched = d1 == d2 and r1 == r2 and bool(d1) and bool(r1)
            partial_matched = (d1 == d2 or r1 == r2) and bool(d1) and bool(r1)

            if domain_matched or partial_matched:
                potential_siblings.append(
                    {
                        "p1_uri": p1_uri,
                        "p2_uri": p2_uri,
                        "p1_label": candidate["p1_label"],
                        "p2_label": candidate["p2_label"],
                        "combined_similarity": candidate["combined_similarity"],
                        "domain_matched": domain_matched,
                    }
                )

        ignored_superprops = {OWL.topObjectProperty, OWL.topDataProperty}

        already_siblings = []
        filtered_potential_siblings = []

        for candidate in potential_siblings:
            p1 = candidate["p1_uri"]
            p2 = candidate["p2_uri"]

            p1_parents = {
                p for p in self.graph.objects(p1, RDFS.subPropertyOf) if isinstance(p, URIRef)
            } - ignored_superprops
            p2_parents = {
                p for p in self.graph.objects(p2, RDFS.subPropertyOf) if isinstance(p, URIRef)
            } - ignored_superprops

            common_parents = p1_parents.intersection(p2_parents)
            record = {
                "p1_uri": str(p1),
                "p2_uri": str(p2),
                "p1_label": candidate["p1_label"],
                "p2_label": candidate["p2_label"],
                "combined_similarity": candidate["combined_similarity"],
                "domain_matched": candidate["domain_matched"],
            }

            if common_parents:
                record["common_parent_labels"] = self._labels_from_uris(common_parents)
                already_siblings.append(record)
            else:
                filtered_potential_siblings.append(record)

        ratio = len(filtered_potential_siblings) / len(self.all_props) if self.all_props else 0.0
        already_siblings_ratio = len(already_siblings) / len(self.all_props) if self.all_props else 0.0
        return {
            "threshold": threshold,
            "already_sibling_count": len(already_siblings),
            "already_siblings_ratio": already_siblings_ratio,
            "already_siblings": already_siblings,
            "count": len(filtered_potential_siblings),
            "ratio": ratio,
            "items": filtered_potential_siblings,
        }

    def run_p2_4(self) -> Dict[str, Any]:
        superproperties = [
            p for p in self.all_props if any(True for _ in self.graph.subjects(RDFS.subPropertyOf, p))
        ]

        singlechild_superproperties = [
            p
            for p in superproperties
            if sum(1 for _ in self.graph.subjects(RDFS.subPropertyOf, p)) == 1
        ]

        items = [
            {
                "property_uri": str(prop),
                "property_label": extract_label(prop, clean=True),
            }
            for prop in singlechild_superproperties
        ]

        ratio = len(items) / len(self.all_props) if self.all_props else 0.0
        return {
            "count": len(items),
            "ratio": ratio,
            "items": items,
        }

    def run_p2_5(self) -> Dict[str, Any]:
        p14_prop_domain = {p: set(self.graph.objects(p, RDFS.domain)) for p in self.oobjprops}
        p14_prop_range = {p: set(self.graph.objects(p, RDFS.range)) for p in self.oobjprops}

        def expand_named_classes(values: Set[Any]) -> Set[URIRef]:
            out: Set[URIRef] = set()
            for value in values:
                if isinstance(value, URIRef):
                    out.add(value)
                else:
                    for head in self.graph.objects(value, OWL.unionOf):
                        for member in self.graph.items(head):
                            if isinstance(member, URIRef):
                                out.add(member)
            return out

        sup_cache: Dict[URIRef, Set[URIRef]] = {}

        def all_superclasses(cls_uri: URIRef) -> Set[URIRef]:
            if cls_uri not in sup_cache:
                supers = {cls_uri}
                supers.update(
                    x
                    for x in self.graph.transitive_objects(cls_uri, RDFS.subClassOf)
                    if isinstance(x, URIRef)
                )
                supers.discard(OWL.Thing)
                sup_cache[cls_uri] = supers
            return sup_cache[cls_uri]

        def common_superclasses(class_uris: Set[URIRef]) -> Set[URIRef]:
            if len(class_uris) < 2:
                return set()
            return set.intersection(*(all_superclasses(c) for c in class_uris))

        multi_domain_same_super = []
        multi_range_same_super = []

        for prop in self.oobjprops:
            domains = expand_named_classes(p14_prop_domain.get(prop, set()))
            if len(domains) > 1:
                common_supers = common_superclasses(domains)
                if common_supers:
                    multi_domain_same_super.append(
                        {
                            "property_uri": str(prop),
                            "property_label": extract_label(prop, clean=True),
                            "domains": self._labels_from_uris(domains),
                            "common_superclasses": self._labels_from_uris(common_supers),
                        }
                    )

            ranges = expand_named_classes(p14_prop_range.get(prop, set()))
            if len(ranges) > 1:
                common_supers = common_superclasses(ranges)
                if common_supers:
                    multi_range_same_super.append(
                        {
                            "property_uri": str(prop),
                            "property_label": extract_label(prop, clean=True),
                            "ranges": self._labels_from_uris(ranges),
                            "common_superclasses": self._labels_from_uris(common_supers),
                        }
                    )

        multi_domain_ratio = len(multi_domain_same_super) / len(self.oobjprops) if self.oobjprops else 0.0
        multi_range_ratio = len(multi_range_same_super) / len(self.oobjprops) if self.oobjprops else 0.0
        return {
            "multi_domain_count": len(multi_domain_same_super),
            "multi_domain_ratio": multi_domain_ratio,
            "multi_domain_items": sorted(multi_domain_same_super, key=lambda x: x["property_label"]),
            "multi_range_count": len(multi_range_same_super),
            "multi_range_ratio": multi_range_ratio,
            "multi_range_items": sorted(multi_range_same_super, key=lambda x: x["property_label"]),
        }

    def run_p3_1(self) -> Dict[str, Any]:
        standard_property_labels = {
            "rdf": list(getattr(RDF, "__annotations__", {}).keys()),
            "rdfs": list(getattr(RDFS, "__annotations__", {}).keys()),
            "owl": list(getattr(OWL, "__annotations__", {}).keys()),
        }

        standard_exact: Dict[str, List[str]] = {}
        standard_normalized: Dict[str, List[str]] = {}

        for ns_prefix, labels in standard_property_labels.items():
            for label in labels:
                qname = f"{ns_prefix}:{label}"
                standard_exact.setdefault(label, []).append(qname)
                standard_normalized.setdefault(normalize_name(label), []).append(qname)

        prop_types = {
            p: ("ObjectProperty" if p in self.oobjprops else "DatatypeProperty")
            for p in self.all_props
        }

        name_matches = []
        for prop in self.all_props:
            local_label = extract_label(prop, clean=False)
            normalized_matches = standard_normalized.get(normalize_name(local_label), [])

            if not normalized_matches:
                continue

            name_matches.append(
                {
                    "property_uri": str(prop),
                    "property_label": local_label,
                    "property_type": prop_types.get(prop, "Unknown"),
                    "matched_standard_props": normalized_matches,
                    "exact_match": bool(standard_exact.get(local_label, [])),
                }
            )

        name_matches = sorted(name_matches, key=lambda x: x["property_label"].lower())

        ratio = len(name_matches) / len(self.all_props) if self.all_props else 0.0
        return {
            "checked_count": len(self.all_props),
            "count": len(name_matches),
            "ratio": ratio,
            "items": name_matches,
        }

    def run_p4_7(self) -> Dict[str, Any]:
        prop_labels_raw = {p: extract_label(p, clean=False) for p in self.odataprops}
        prop_labels_norm = {p: normalize_name(label) for p, label in prop_labels_raw.items()}

        overlap_pairs = []

        for i, p1 in enumerate(self.odataprops):
            n1 = prop_labels_norm[p1]
            for p2 in self.odataprops[i + 1:]:
                n2 = prop_labels_norm[p2]

                if not n1 or not n2 or n1 == n2:
                    continue

                if n1 in n2 or n2 in n1:
                    short_p, long_p = (p1, p2) if len(n1) <= len(n2) else (p2, p1)
                    if len(prop_labels_norm[short_p]) < 4:
                        continue

                    overlap_pairs.append(
                        {
                            "short_uri": str(short_p),
                            "long_uri": str(long_p),
                            "short_label": prop_labels_raw[short_p],
                            "long_label": prop_labels_raw[long_p],
                        }
                    )

        overlap_pairs = sorted(overlap_pairs, key=lambda x: (x["short_label"].lower(), x["long_label"].lower()))

        ratio = len(overlap_pairs) / len(self.odataprops) if self.odataprops else 0.0
        return {
            "count": len(overlap_pairs),
            "ratio": ratio,
            "items": overlap_pairs,
        }

    def run_p3_2(self) -> Dict[str, Any]:
        range_in_title = []

        for prop in self.oobjprops:
            ranges = list(self.graph.objects(prop, RDFS.range))
            prop_label = extract_label(prop, clean=True).lower()
            if not ranges:
                continue

            for range_cls in ranges:
                if not isinstance(range_cls, URIRef):
                    continue

                range_label = extract_label(range_cls, clean=True).lower()
                if prop_label.endswith(range_label):
                    range_in_title.append(
                        {
                            "property_uri": str(prop),
                            "property_label": extract_label(prop, clean=False),
                            "range_uri": str(range_cls),
                            "range_label": extract_label(range_cls, clean=False),
                        }
                    )

        ratio = len(range_in_title) / len(self.oobjprops) if self.oobjprops else 0.0
        return {
            "count": len(range_in_title),
            "ratio": ratio,
            "items": range_in_title,
        }

    def run_p3_3(self) -> Dict[str, Any]:
        domain_in_title = []

        for prop in self.oobjprops:
            domains = list(self.graph.objects(prop, RDFS.domain))
            prop_label = extract_label(prop, clean=True).lower()
            if not domains:
                continue

            for domain_cls in domains:
                if not isinstance(domain_cls, URIRef):
                    continue

                domain_label = extract_label(domain_cls, clean=True).lower()
                if prop_label.startswith(domain_label):
                    domain_in_title.append(
                        {
                            "property_uri": str(prop),
                            "property_label": extract_label(prop, clean=False),
                            "domain_uri": str(domain_cls),
                            "domain_label": extract_label(domain_cls, clean=False),
                        }
                    )

        ratio = len(domain_in_title) / len(self.oobjprops) if self.oobjprops else 0.0
        return {
            "count": len(domain_in_title),
            "ratio": ratio,
            "items": domain_in_title,
        }

    def run_p1_3(self) -> Dict[str, Any]:
        ontology_uris = [s for s in self.graph.subjects(RDF.type, OWL.Ontology)]
        ontology_uri = str(ontology_uris[0]) if ontology_uris else self.ontology_path.as_uri()

        try:
            from owlready2 import World, sync_reasoner
        except Exception as exc:
            return {
                "ontology_uri": ontology_uri,
                "error": f"Could not import owlready2: {exc}",
            }

        try:
            world = World()
            onto = world.get_ontology(ontology_uri)

            serialized = self.graph.serialize(format="xml")
            if isinstance(serialized, str):
                serialized = serialized.encode("utf-8")

            onto.load(fileobj=io.BytesIO(serialized), format="rdfxml")

            with onto:
                sync_reasoner()

            inconsistent_classes = [str(c) for c in world.inconsistent_classes()]
            ratio = len(inconsistent_classes) / len(self.oclasses) if self.oclasses else 0.0
            return {
                "ontology_uri": ontology_uri,
                "count": len(inconsistent_classes),
                "ratio": ratio,
                "items": inconsistent_classes,
            }
        except Exception as exc:
            return {
                "ontology_uri": ontology_uri,
                "error": str(exc),
            }
