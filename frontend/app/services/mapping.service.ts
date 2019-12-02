import { Injectable } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { DataService } from './data.service';
import { ToastrService } from 'ngx-toastr';


@Injectable()
export class MappingService {

    public fieldMappingForm: FormGroup;
    public userMapping;
    public columns;
	public newMapping: boolean = false;
	public id_mapping;


    constructor(
        private _ds: DataService, 
        private toastr: ToastrService
    ) {}


	getMappingNamesList(mapping_type, importId) {
		this._ds.getMappings(mapping_type, importId).subscribe(
			(result) => {
                this.userMapping = result['mappings'];
                if (result['column_names'] != 'undefined import_id') {
                    this.columns = result['column_names'].map(
                        (col) => {
                            return {
                                id: col,
                                selected: false
                            };
                        });
                }
                console.log(this.userMapping);
                
			},
			(error) => {
				console.error(error);
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					this.toastr.error(error.error);
				}
			}
		);
    }


    createMapping() {
		this.fieldMappingForm.reset();
		this.newMapping = true;
	}


	cancelMapping() {
		this.newMapping = false;
		this.fieldMappingForm.controls['mappingName'].setValue('');
	}
	
		
	saveMappingName(value, importId, targetForm) {
        let mappingType = 'FIELD';
		this._ds.postMappingName(value, mappingType).subscribe(
			(res) => {
				this.newMapping = false;
				this.getMappingNamesList(mappingType, importId);
				this.fieldMappingForm.controls['fieldMapping'].setValue(res);
				this.fieldMappingForm.controls['mappingName'].setValue('');
				this.enableMapping(targetForm);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.error(error);
					this.toastr.error(error.error);
				}
			}
		);
	}
    

    onMappingName(targetFormName): void {
		this.fieldMappingForm.get('fieldMapping').valueChanges.subscribe(
			(id_mapping) => {
				this.id_mapping = id_mapping;
				if (this.id_mapping) {
					this.fillMapping(this.id_mapping, targetFormName);
				} else {
					this.fillEmptyMapping(targetFormName);
					this.getSelectedOptions(this.id_mapping, targetFormName);
				}
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.error(error);
					this.toastr.error(error.error);
				}
			}
		);
    }
    

	fillMapping(id_mapping, targetFormName) {
		this.id_mapping = id_mapping;
		this._ds.getMappingFields(this.id_mapping).subscribe(
			(mappingFields) => {
				if (mappingFields[0] != 'empty') {
					for (let field of mappingFields) {
						targetFormName.controls[field['target_field']].enable();
						targetFormName.get(field['target_field']).setValue(field['source_field']);
					}
					this.getSelectedOptions(this.id_mapping, targetFormName);
				} else {
					this.fillEmptyMapping(targetFormName);
				}
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.error(error);
					this.toastr.error(error.error);
				}
			}
		);
    }
    

    getSelectedOptions(id_mapping, targetFormName) {
		let formValues = targetFormName.value;
		if (id_mapping == undefined || id_mapping == '') {
			this.disableMapping(targetFormName);
		} else {
			this.columns.map((col) => {
				if (formValues) {
					if (Object.values(formValues).includes(col.id)) {
						col.selected = true;
					} else {
						col.selected = false;
					}
				}
			});
		}
		
    }


	onSelect(id_mapping, targetFormName) {
		this.id_mapping = id_mapping;
		this.getSelectedOptions(this.id_mapping, targetFormName);
	}


	disableMapping(targetForm) {
		Object.keys(targetForm.controls).forEach((key) => {
			targetForm.controls[key].disable();
		});
	}


	enableMapping(targetForm) {
		Object.keys(targetForm.controls).forEach((key) => {
			targetForm.controls[key].enable();
		});
	}


	fillEmptyMapping(targetForm) {
		Object.keys(targetForm.controls).forEach((key) => {
			targetForm.get(key).setValue('');
		});
	}
	
    
}