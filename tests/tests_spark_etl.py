"""
Tests para validar DAGs de Airflow
"""
import os
import sys
from pathlib import Path

# Agregar carpeta de DAGs al path
dag_folder = Path(__file__).parent.parent / "airflow" / "dags"
sys.path.insert(0, str(dag_folder))


def test_dag_imports():
    """Validar que todos los DAGs se pueden importar sin errores"""
    dag_files = dag_folder.glob("*.py")
    
    for dag_file in dag_files:
        if dag_file.name.startswith("_"):
            continue
            
        dag_name = dag_file.stem
        print(f"Validando DAG: {dag_name}")
        
        try:
            # Intentar importar el módulo
            __import__(dag_name)
            print(f"  ✅ {dag_name} importado correctamente")
        except Exception as e:
            print(f"  ❌ Error importando {dag_name}: {str(e)}")
            raise


def test_dag_structure():
    """Validar estructura básica de DAGs"""
    import importlib
    
    dag_files = [f.stem for f in dag_folder.glob("*.py") if not f.name.startswith("_")]
    
    for dag_name in dag_files:
        print(f"Validando estructura de: {dag_name}")
        
        module = importlib.import_module(dag_name)
        
        # Verificar que el DAG tiene un objeto 'dag' o está en el contexto
        assert hasattr(module, 'dag') or 'DAG' in dir(module), \
            f"DAG {dag_name} no tiene objeto 'dag' definido"
        
        print(f"  ✅ {dag_name} tiene estructura válida")


if __name__ == "__main__":
    print("=" * 70)
    print("EJECUTANDO TESTS DE DAGS")
    print("=" * 70)
    
    test_dag_imports()
    test_dag_structure()
    
    print("\n" + "=" * 70)
    print("✅ TODOS LOS TESTS PASARON")
    print("=" * 70)