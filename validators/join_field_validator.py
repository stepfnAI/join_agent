from typing import Dict, Any, Set
import pandas as pd
import numpy as np
from datetime import datetime

class JoinFieldValidator:
    def __init__(self, table1: pd.DataFrame, table2: pd.DataFrame):
        self.table1 = table1
        self.table2 = table2
    
    def uniqueness_check(self, field1: str, field2: str) -> Dict[str, Any]:
        """Check if proposed join fields create unique records"""
        def check_uniqueness(df: pd.DataFrame, field: str) -> Dict:
            total = len(df)
            unique = df[field].nunique()
            return {
                "check_type": "uniqueness_check",
                "description": "Verify if proposed join fields create unique records",
                "metrics": {
                    "total_records": total,
                    "unique_combinations": unique,
                    "duplication_rate": round((1 - (unique / total)) * 100, 2)
                }
            }
        
        return {
            'table1_metrics': check_uniqueness(self.table1, field1),
            'table2_metrics': check_uniqueness(self.table2, field2)
        }

    def pattern_match_check(self, field1: str, field2: str) -> Dict[str, Any]:
        """Analyze value patterns across tables"""
        def analyze_patterns(series: pd.Series) -> Dict:
            sample = series.dropna().astype(str)
            return {
                "check_type": "pattern_match",
                "description": "Analyze if values follow similar patterns across tables",
                "checks": {
                    "format_consistency": bool(sample.str.match(sample.iloc[0].replace('\\', '\\\\')).all()),
                    "length_match": bool(sample.str.len().nunique() == 1),
                    "character_types": self._get_character_type(sample)
                }
            }
        
        return {
            'table1_pattern': analyze_patterns(self.table1[field1]),
            'table2_pattern': analyze_patterns(self.table2[field2])
        }

    def date_compatibility_check(self, field1: str, field2: str) -> Dict[str, Any]:
        """Verify date field compatibility"""
        def analyze_dates(series: pd.Series) -> Dict:
            dates = pd.to_datetime(series, errors='coerce')
            return {
                "check_type": "date_compatibility",
                "description": "Verify date field compatibility and overlap",
                "checks": {
                    "format_match": self._check_date_format(series),
                    "range_overlap": self._calculate_date_overlap(dates),
                    "granularity": self._determine_date_granularity(dates)
                }
            }
        
        return {
            'table1_dates': analyze_dates(self.table1[field1]),
            'table2_dates': analyze_dates(self.table2[field2])
        }

    def null_analysis(self, field1: str, field2: str) -> Dict[str, Any]:
        """Analyze null patterns"""
        def analyze_nulls(series: pd.Series) -> Dict:
            return {
                "check_type": "null_analysis",
                "description": "Analyze null patterns in join fields",
                "metrics": {
                    "null_percentage": float(series.isnull().mean() * 100),
                    "null_distribution": self._analyze_null_distribution(series)
                }
            }
        
        return {
            'table1_nulls': analyze_nulls(self.table1[field1]),
            'table2_nulls': analyze_nulls(self.table2[field2])
        }

    def cardinality_check(self, field1: str, field2: str) -> Dict[str, Any]:
        """Analyze relationship cardinality"""
        return {
            "check_type": "cardinality",
            "description": "Understand relationship between tables",
            "metrics": {
                "relationship_type": self._determine_relationship_type(field1, field2),
                "max_fan_out": self._calculate_max_fan_out(field1, field2),
                "distribution": self._calculate_relationship_distribution(field1, field2)
            }
        }

    def value_overlap_check(self, field1: str, field2: str) -> Dict[str, Any]:
        """Analyze value overlap between tables"""
        values1 = set(self.table1[field1].dropna().unique())
        values2 = set(self.table2[field2].dropna().unique())
        overlap = values1.intersection(values2)
        
        return {
            "check_type": "value_overlap",
            "description": "Analyze how many values exist in both tables",
            "metrics": {
                "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
                "unique_to_table1": len(values1 - overlap),
                "unique_to_table2": len(values2 - overlap)
            }
        }

    def run_all_checks(self, field1: str, field2: str, is_date: bool = False) -> Dict[str, Any]:
        """Run all validation checks"""
        results = {
            'uniqueness': self.uniqueness_check(field1, field2),
            'pattern_match': self.pattern_match_check(field1, field2),
            'null_analysis': self.null_analysis(field1, field2),
            'cardinality': self.cardinality_check(field1, field2),
            'value_overlap': self.value_overlap_check(field1, field2)
        }
        
        if is_date:
            results['date_compatibility'] = self.date_compatibility_check(field1, field2)
        
        return results

    # Helper methods
    def _get_character_type(self, series: pd.Series) -> str:
        if series.str.match(r'^\d+$').all():
            return 'numeric'
        elif series.str.match(r'^[a-zA-Z]+$').all():
            return 'alphabetic'
        return 'alphanumeric'

    def _check_date_format(self, series: pd.Series) -> bool:
        return pd.to_datetime(series, errors='coerce').notnull().all()

    def _calculate_date_overlap(self, dates: pd.Series) -> float:
        if dates.empty:
            return 0.0
        date_range = dates.max() - dates.min()
        return (date_range.days / 365.25) if date_range else 0.0

    def _determine_date_granularity(self, dates: pd.Series) -> str:
        if dates.empty:
            return 'unknown'
        if (dates.dt.hour != 0).any():
            return 'timestamp'
        return 'date'

    def _analyze_null_distribution(self, series: pd.Series) -> Dict:
        return {
            'start': float(series.head(100).isnull().mean()),
            'middle': float(series.iloc[len(series)//2-50:len(series)//2+50].isnull().mean()),
            'end': float(series.tail(100).isnull().mean())
        }

    def _determine_relationship_type(self, field1: str, field2: str) -> str:
        card1 = self.table1[field1].value_counts()
        card2 = self.table2[field2].value_counts()
        
        if card1.max() <= 1 and card2.max() <= 1:
            return 'one-to-one'
        elif card1.max() <= 1:
            return 'one-to-many'
        elif card2.max() <= 1:
            return 'many-to-one'
        return 'many-to-many'

    def _calculate_max_fan_out(self, field1: str, field2: str) -> int:
        return max(
            self.table1[field1].value_counts().max(),
            self.table2[field2].value_counts().max()
        )

    def _calculate_relationship_distribution(self, field1: str, field2: str) -> Dict:
        return {
            'table1_distribution': self.table1[field1].value_counts().describe().to_dict(),
            'table2_distribution': self.table2[field2].value_counts().describe().to_dict()
        }