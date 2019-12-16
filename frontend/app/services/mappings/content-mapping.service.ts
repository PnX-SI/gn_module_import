import { Injectable } from '@angular/core';
import { FormGroup, FormControl } from '@angular/forms';
import { DataService } from '../data.service';
import { ToastrService } from 'ngx-toastr';


@Injectable()
export class ContentMappingService {

    public userContentMappings;
	public newMapping: boolean = false;
    public id_mapping;

    constructor(
        private _ds: DataService, 
        private toastr: ToastrService
    ) {}


    getMappingNamesList(mapping_type, importId) {
        // get list of existing content mapping in the select
		this._ds.getMappings(mapping_type, importId).subscribe(
			(result) => {
                this.userContentMappings = result['mappings'];
			},
			(error) => {
				console.log(error);
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error);
				}
			}
		);
    }


    createMapping(mappingForm) {
		mappingForm.reset();
		this.newMapping = true;
	}


	cancelMapping(mappingForm) {
		this.newMapping = false;
		mappingForm.controls['mappingName'].setValue('');
    }


    saveMappingName(value, importId, mappingForm) {
        // save new mapping in bib_mapping
        // then select the mapping name in the select
        let mappingType = 'CONTENT';
		this._ds.postMappingName(value, mappingType).subscribe(
			(res) => {
				this.newMapping = false;
				this.getMappingNamesList(mappingType, importId);
				mappingForm.controls['contentMapping'].setValue(res);
                mappingForm.controls['mappingName'].setValue('');
                //this.enableMapping(targetForm);
			},
			(error) => {
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error);
				}
			}
		);
    }
    
}